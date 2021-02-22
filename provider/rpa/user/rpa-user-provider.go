package main // import "github.com/synerex/rpa_user"

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync"

	"github.com/jinzhu/gorm"
	"golang.org/x/crypto/bcrypt"

	rpa "github.com/synerex/proto_rpa"
	api "github.com/synerex/synerex_api"
	proto "github.com/synerex/synerex_proto"
	sxutil "github.com/synerex/synerex_sxutil"

	"github.com/gin-contrib/sessions"
	"github.com/gin-contrib/sessions/cookie"
	"github.com/gin-gonic/gin"
	_ "github.com/mattn/go-sqlite3"
	gosocketio "github.com/mtfelian/golang-socketio"
	"github.com/tidwall/gjson"
)

var (
	nodesrv         = flag.String("nodesrv", "127.0.0.1:9990", "Node ID Server")
	spMap           map[uint64]*api.Supply
	mu              sync.RWMutex
	port            = flag.Int("port", 7777, "RPA User Provider Listening Port")
	server          = gosocketio.NewServer()
	rm              *rpa.MeetingService
	sxServerAddress string
	uid             uint
)

func init() {
	spMap = make(map[uint64]*api.Supply)
}

func confirmBooking(clt *sxutil.SXServiceClient, sp *api.Supply) {
	spMap[sp.Id] = sp

	// emit to client
	channel, err := server.GetChannel(rm.Cid)
	if err != nil {
		fmt.Println("Failed to get socket channel:", err)
	}
	js := `{"id":"` + strconv.FormatUint(sp.Id, 10) + `","year":"` + rm.Year + `","month":"` + rm.Month + `","day":"` + rm.Day + `","week":"` + rm.Week + `","start":"` + rm.Start + `","end":"` + rm.End + `","people":"` + rm.People + `","title":"` + rm.Title + `","room":"` + rm.Room + `"}`

	// 暫定的
	log.Println("js:", js)

	channel.Emit("check_booking", js)

	server.On("confirm_booking", func(c *gosocketio.Channel, data interface{}) {
		switch d := data.(type) {
		case string:
			uintData, err := strconv.ParseUint(d, 0, 64)
			if err != nil {
				fmt.Println("Failed to parse uint64:", err)
			}
			clt.SelectSupply(spMap[uintData])
			channel.Emit("server_to_client", spMap[uintData])
		}
	})
}

func setMeetingService(json string) {
	cid := gjson.Get(json, "cid").String()
	status := gjson.Get(json, "status").String()
	year := gjson.Get(json, "year").String()
	month := gjson.Get(json, "month").String()
	day := gjson.Get(json, "day").String()
	week := gjson.Get(json, "week").String()
	start := gjson.Get(json, "start").String()
	end := gjson.Get(json, "end").String()
	people := gjson.Get(json, "people").String()
	title := gjson.Get(json, "title").String()
	room := gjson.Get(json, "room").String()

	rm = &rpa.MeetingService{
		Cid:    cid,
		Status: status,
		Year:   year,
		Month:  month,
		Day:    day,
		Week:   week,
		Start:  start,
		End:    end,
		People: people,
		Title:  title,
		Room:   room,
	}
}

func supplyCallback(clt *sxutil.SXServiceClient, sp *api.Supply) {
	log.Println("Got RPA User supply callback")
	setMeetingService(sp.ArgJson)

	if rm.People == "" {
		rm.People = "0"
	}

	switch rm.Status {
	case "OK":
		confirmBooking(clt, sp)
	case "NG":
		// emit to client
		channel, err := server.GetChannel(rm.Cid)
		if err != nil {
			fmt.Println("Failed to get socket channel:", err)
		}
		msg := "NG from id:" + strconv.FormatUint(sp.Id, 10)
		channel.Emit("server_to_client", msg)
	default:
		fmt.Printf("Switch case of default(%s) is called\n", rm.Status)
	}
}

func subscribeSupply(client *sxutil.SXServiceClient) {
	//called as goroutine
	ctx := context.Background() // should check proper context
	client.SubscribeSupply(ctx, supplyCallback)
	// comes here if channel closed
	log.Println("SMarket Server Closed?")
}

func runSocketIOServer(sclient *sxutil.SXServiceClient) {
	server.On(gosocketio.OnConnection, func(c *gosocketio.Channel) {
		log.Printf("Connected %s\n", c.Id())
	})

	server.On("client_to_server", func(c *gosocketio.Channel, data interface{}) {
		log.Println("client_to_server:", data)
		byte, _ := json.Marshal(data)
		st := string(byte)
		year := gjson.Get(st, "Year").String()
		month := gjson.Get(st, "Month").String()
		day := gjson.Get(st, "Day").String()
		week := gjson.Get(st, "Week").String()
		start := gjson.Get(st, "Start").String()
		end := gjson.Get(st, "End").String()
		people := gjson.Get(st, "People").String()
		title := gjson.Get(st, "Title").String()

		// insert record to db
		insertReservation(year, month, day, week, start, end, people, title, uid)

		// get reseravtion id
		ridUint := getReservationIDFromUserID(uid)
		rid := strconv.FormatUint(uint64(ridUint), 10)

		rm := rpa.MeetingService{
			Cid:    c.Id(),
			Status: "checking",
			Year:   year,
			Month:  month,
			Day:    day,
			Week:   week,
			Start:  start,
			End:    end,
			People: people,
			Title:  title,
			Rid:    rid,
		}
		b, _ := json.Marshal(rm)
		sendDemand(sclient, "Booking meeting room", string(b))
	})

	server.On(gosocketio.OnDisconnection, func(c *gosocketio.Channel) {
		log.Printf("Disconnected %s\n", c.Id())
	})

	serveMux := http.NewServeMux()
	serveMux.Handle("/socket.io/", server)
	serveMux.Handle("/", http.FileServer(http.Dir("./client")))

	log.Printf("Starting Server at localhost:%d\n", *port)
	if err := http.ListenAndServe(fmt.Sprintf(":%d", *port), serveMux); err != nil {
		log.Fatal(err)
	}
}

func sendDemand(sclient *sxutil.SXServiceClient, nm string, js string) {
	opts := &sxutil.DemandOpts{Name: nm, JSON: js}
	mu.Lock()
	//TODO: should cover error for demand.
	id, _ := sclient.NotifyDemand(opts)
	mu.Unlock()
	log.Printf("Register meeting demand as id:%v\n", id)
}

// Web Framework Gin
func runGinServer() {
	gin.SetMode(gin.ReleaseMode)

	route := gin.Default()
	log.Println("Starting User Server at localhost:8888")

	// init User DB
	initUserDB()

	// Template
	route.Static("client", "./client")
	route.LoadHTMLGlob("client/gin-templates/*.html")

	// Session
	store := cookie.NewStore([]byte("secret"))
	route.Use(sessions.Sessions("mysession", store))

	// Create
	route.GET("/new", func(c *gin.Context) {
		c.HTML(200, "new.html", gin.H{})
	})
	route.POST("/new", func(c *gin.Context) {
		username := c.PostForm("username")
		password := c.PostForm("password")
		if isIdenticalUsername(username) {
			insertUser(username, password)
			c.HTML(200, "lockscreen.html", gin.H{
				"username": username,
			})
		} else {
			c.HTML(http.StatusBadRequest, "new.html", gin.H{"message": "Invalid username"})
		}
	})

	// Login
	route.GET("/login", func(c *gin.Context) {
		c.HTML(200, "login.html", gin.H{"message": ""})
	})
	route.POST("/login", login)
	route.GET("/logout", logout)
	authorized := route.Group("/")
	authorized.Use(AuthRequired())
	{
		authorized.GET("/", func(c *gin.Context) {
			c.HTML(200, "index.html", gin.H{"message": "You are authorized now."})
		})

		// Index
		authorized.GET("/users", func(c *gin.Context) {
			users := getAllUser()
			c.HTML(200, "users.html", gin.H{
				"users": users,
			})
		})

		// Show
		authorized.GET("/show/:id", func(c *gin.Context) {
			p := c.Param("id")
			id, err := strconv.Atoi(p)
			if err != nil {
				fmt.Println("Failed to strconv:", err)
			}
			user := getOneUser(id)
			reservations := getAllReservations(id)
			c.HTML(200, "show.html", gin.H{
				"user":         user,
				"reservations": reservations,
			})
		})

		// // Update
		// authorized.POST("/update/:id", func(c *gin.Context) {
		// 	p := c.Param("id")
		// 	id, err := strconv.Atoi(p)
		// 	if err != nil {
		// 		fmt.Println("Failed to strconve:", err)
		// 	}
		// 	username := c.PostForm("username")
		// 	password := c.PostForm("password")
		// 	updateUser(id, username, password)
		// 	c.Redirect(302, "/")
		// })

		// Confirm Delete
		authorized.GET("/confirm_delete/:id", func(c *gin.Context) {
			p := c.Param("id")
			id, err := strconv.Atoi(p)
			if err != nil {
				fmt.Println("Failed to strconv:", err)
			}
			user := getOneUser(id)
			c.HTML(200, "delete.html", gin.H{
				"user": user,
			})
		})

		// Cancel Reservation
		authorized.POST("/cancel/:id", func(c *gin.Context) {
			p := c.Param("id")
			id, err := strconv.Atoi(p)
			if err != nil {
				log.Fatalln(err)
			}
			cancelReservation(id)
			c.Redirect(302, "/users")
		})

		// Delete
		authorized.POST("/delete/:id", func(c *gin.Context) {
			p := c.Param("id")
			id, err := strconv.Atoi(p)
			if err != nil {
				fmt.Println("Failed to strconv:", err)
			}
			deleteUser(id)

			session := sessions.Default(c)
			user := session.Get("user")
			if user == nil {
				c.JSON(400, gin.H{"error": "Invalid session token"})
			} else {
				session.Delete("user")
				session.Save()
			}
			c.Redirect(302, "/")
		})
	}

	route.Run(":8888")
}

func AuthRequired() gin.HandlerFunc {
	return func(c *gin.Context) {
		session := sessions.Default(c)
		user := session.Get("user")
		if user == nil {
			c.HTML(http.StatusUnauthorized, "login.html", gin.H{"message": "You must login first."})
			c.Abort()
		} else {
			c.Next()
		}
	}
}

func login(c *gin.Context) {
	session := sessions.Default(c)
	username := c.PostForm("username")
	password := c.PostForm("password")

	if strings.Trim(username, " ") == "" || strings.Trim(password, " ") == "" {
		c.HTML(http.StatusBadRequest, "login.html", gin.H{"message": "Parameters cannot be empty."})
		return
	}

	uid = getUserIDFromName(username)
	user := getOneUser(int(uid))
	if username == user.Username {
		if err := verify(user.Password, password); err != nil {
			c.HTML(http.StatusUnauthorized, "login.html", gin.H{"message": "Authentication failed"})
		} else {
			session.Set("user", username)
			err := session.Save()
			if err != nil {
				fmt.Println("Failed to generate session token:", err)
				c.HTML(http.StatusInternalServerError, "login.html", gin.H{"message": "Failed to login."})
			} else {
				c.Redirect(302, "http://localhost:7777/")
			}
		}
	} else {
		c.HTML(http.StatusUnauthorized, "login.html", gin.H{"message": "Authentication failed"})
	}
}

func logout(c *gin.Context) {
	session := sessions.Default(c)
	user := session.Get("user")
	if user == nil {
		c.JSON(400, gin.H{"error": "Invalid session token"})
	} else {
		fmt.Println("Logged out:", user)
		session.Delete("user")
		session.Save()
		c.Redirect(302, "/")
	}
}

func hash(pass string) (string, error) {
	hash, err := bcrypt.GenerateFromPassword([]byte(pass), bcrypt.DefaultCost)
	if err != nil {
		return "", err
	}
	return string(hash), err
}

func verify(hash, pass string) error {
	return bcrypt.CompareHashAndPassword([]byte(hash), []byte(pass))
}

type User struct {
	gorm.Model
	Username     string
	Password     string
	Reservations []Reservation
}
type Reservation struct {
	gorm.Model
	Year     string
	Month    string
	Day      string
	Week     string
	Start    string
	End      string
	People   string
	Title    string
	RoomName string
	Active   bool
	UserID   uint
	Proposes []Propose
}
type Propose struct {
	gorm.Model
	ServiceName   string
	RoomName      string
	UserID        uint
	ReservationID uint
}

func initUserDB() {
	db, err := gorm.Open("sqlite3", "user.sqlite3")
	if err != nil {
		fmt.Println("Failed to open gorm:", err)
	}

	// テーブルが存在していた場合は削除
	db.DropTableIfExists(&User{})
	db.DropTableIfExists(&Reservation{})
	db.DropTableIfExists(&Propose{})

	db.AutoMigrate(&User{})
	db.AutoMigrate(&Reservation{})
	db.AutoMigrate(&Propose{})
	defer db.Close()
}

func insertUser(username string, password string) {
	db, err := gorm.Open("sqlite3", "user.sqlite3")
	if err != nil {
		fmt.Println("Failed to open gorm:", err)
	}
	hash, err := hash(password)
	if err != nil {
		fmt.Println("Failed to hash at insertUser:", err)
	}
	db.Create(&User{Username: username, Password: hash})
	defer db.Close()
}

func insertReservation(year string, month string, day string, week string, start string, end string, people string, title string, uid uint) {
	db, err := gorm.Open("sqlite3", "user.sqlite3")
	if err != nil {
		fmt.Println("Failed to open gorm:", err)
	}
	db.Create(&Reservation{
		Year:   year,
		Month:  month,
		Day:    day,
		Week:   week,
		Start:  start,
		End:    end,
		People: people,
		Title:  title,
		Active: true,
		UserID: uid,
	})
	defer db.Close()
}

func insertPropose(service string, room string, uid uint, rid uint) {
	db, err := gorm.Open("sqlite3", "user.sqlite3")
	if err != nil {
		log.Fatalln(err)
	}
	db.Create(&Propose{
		ServiceName:   service,
		RoomName:      room,
		UserID:        uid,
		ReservationID: rid,
	})
	defer db.Close()
}

// func updateUser(id int, username string, password string) {
// 	db, err := gorm.Open("sqlite3", "user.sqlite3")
// 	if err != nil {
// 		fmt.Println("Failed to open gorm:", err)
// 	}
// 	hash, err := hash(password)
// 	if err != nil {
// 		fmt.Println("Failed to hash at updateUser:", err)
// 	}
// 	var user User
// 	db.First(&user, id)
// 	user.Username = username
// 	user.Password = hash
// 	db.Save(&user)
// 	defer db.Close()
// }

func cancelReservation(id int) {
	db, err := gorm.Open("sqlite3", "user.sqlite3")
	if err != nil {
		fmt.Println("Failed to open gorm:", err)
	}
	var reservation Reservation
	db.First(&reservation, id)
	reservation.Active = false
	db.Save(&reservation)
	defer db.Close()
}

func deleteUser(id int) {
	db, err := gorm.Open("sqlite3", "user.sqlite3")
	if err != nil {
		fmt.Println("Failed to open gorm:", err)
	}
	var user User
	var reservation Reservation
	var propose Propose
	db.First(&user)
	db.Where("user_id = ?", user.ID).Delete(&reservation)
	db.Where("user_id = ?", user.ID).Delete(&propose)
	db.Delete(&user)
	defer db.Close()
}

func getAllUser() []User {
	db, err := gorm.Open("sqlite3", "user.sqlite3")
	if err != nil {
		fmt.Println("Failed to open gorm:", err)
	}
	var users []User
	db.Order("created_at desc").Find(&users)
	db.Close()
	return users
}

func getOneUser(id int) User {
	db, err := gorm.Open("sqlite3", "user.sqlite3")
	if err != nil {
		fmt.Println("Failed to open gorm:", err)
	}
	var user User
	db.First(&user, id)
	db.Close()
	return user
}

func getAllReservations(userid int) []Reservation {
	db, err := gorm.Open("sqlite3", "user.sqlite3")
	if err != nil {
		fmt.Println("Failed to open gorm:", err)
	}
	var reservations []Reservation
	user := getOneUser(userid)
	db.Model(&user).Related(&reservations)
	return reservations
}

func getOneReservation(id int) Reservation {
	db, err := gorm.Open("sqlite3", "user.sqlite3")
	if err != nil {
		fmt.Println("Failed to open gorm:", err)
	}
	var reservation Reservation
	db.First(&reservation, id)
	db.Close()
	return reservation
}

func getReservationIDFromUserID(uid uint) uint {
	db, err := gorm.Open("sqlite3", "user.sqlite3")
	if err != nil {
		fmt.Println("Failed to open gorm:", err)
	}
	var reservation Reservation
	db.Where("user_id = ?", uid).First(&reservation)
	return reservation.ID
}

func getUserIDFromName(username string) uint {
	db, err := gorm.Open("sqlite3", "user.sqlite3")
	if err != nil {
		fmt.Println("Failed to open gorm:", err)
	}
	var user User
	db.Where("username = ?", username).First(&user)
	db.Close()
	return user.ID
}

func isIdenticalUsername(username string) bool {
	users := getAllUser()
	for _, user := range users {
		if user.Username == username {
			return false
		}
	}
	return true
}

func main() {
	log.Printf("RPA_User(%s) built %s sha1 %s", sxutil.GitVer, sxutil.BuildTime, sxutil.Sha1Ver)
	flag.Parse()

	go sxutil.HandleSigInt()
	sxutil.RegisterDeferFunction(sxutil.UnRegisterNode)

	channelTypes := []uint32{proto.MEETING_SERVICE}
	// obtain synerex server address from nodeserv
	srv, err := sxutil.RegisterNode(*nodesrv, "RPAUserProvider", channelTypes, nil)
	if err != nil {
		log.Fatal("Can't register node...")
	}
	log.Printf("Connecting Server [%s]\n", srv)

	wg := sync.WaitGroup{} // for syncing other goroutines
	sxServerAddress = srv
	client := sxutil.GrpcConnectServer(srv)
	argJson := fmt.Sprintf("{Client:RPAUser}")
	sclient := sxutil.NewSXServiceClient(client, proto.MEETING_SERVICE, argJson)

	wg.Add(1)
	go subscribeSupply(sclient)

	wg.Add(1)
	go runSocketIOServer(sclient)

	wg.Add(1)
	go runGinServer()

	wg.Wait()
	sxutil.CallDeferFunctions() // cleanup!
}

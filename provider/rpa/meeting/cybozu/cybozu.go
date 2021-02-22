package cybozu // import "github.com/synerex/meeting_cybozu"

import (
	"errors"
	"fmt"
	"log"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/PuerkitoBio/goquery"

	"github.com/sclevine/agouti"
)

var (
	url       = "https://onlinedemo.cybozu.info/scripts/office10/ag.cgi?"
	loginName = "高橋 健太"
)

type Detail struct {
	Memo         string
	OccupiedTime string
}

func getPageDOM(page *agouti.Page) *goquery.Document {
	// get whole page
	wholePage, err := page.HTML()
	if err != nil {
		fmt.Println("Failed to get whole page:", err)
	}
	// use goquery
	readerOfPage := strings.NewReader(wholePage)
	pageDom, err := goquery.NewDocumentFromReader(readerOfPage)
	if err != nil {
		fmt.Println("Failed to get page dom:", err)
	}
	return pageDom
}

func searchIndex(dates []string, target string) (int, error) {
	index := -1
	for i, date := range dates {
		if date == target {
			index = i
		}
		// fmt.Println(i, date)
	}
	if index == -1 {
		errMsg := "Failed to set parameter: " + target
		return -1, errors.New(errMsg)
	} else {
		return index, nil
	}
}

func login(page *agouti.Page, user string) error {
	// get user list
	usersDom := getPageDOM(page).Find("select[name='_ID']").Children()
	users := make([]string, usersDom.Length())
	usersDom.Each(func(i int, sel *goquery.Selection) {
		users[i] = sel.Text()
		// fmt.Println(i, users[i])
	})
	// search index
	userIndex, err := searchIndex(users, user)
	if err != nil {
		return err
	}
	// set login user
	name := page.FindByName("_ID")
	if _, err := name.Count(); err != nil {
		return err
	}
	name.Select(users[userIndex])
	// click login button
	submitBtn := page.FindByName("Submit")
	if _, err := submitBtn.Count(); err != nil {
		return err
	}
	// click
	if err := submitBtn.Click(); err != nil {
		return err
	}
	return nil
}

func clickBtnByPath(page *agouti.Page, path string) error {
	btn := page.Find(path)
	if _, err := btn.Count(); err != nil {
		return err
	}
	btn.Click()
	time.Sleep(1 * time.Second)
	return nil
}

func comparePeriod(page *agouti.Page, year string, month string, day string) error {
	screenDate, err := checkScreenDate(page, "#cb7-schedweek-date-title233 > b")
	if err != nil {
		return err
	}

	targetY, targetM, targetD := extractDate(screenDate)

	targetObj := createTimeDate(targetY, targetM, targetD)
	sentObj := createTimeDate(year, month, day)

	// 送られてきた日付の画面まで遷移する
	hour := subtractHour(sentObj, targetObj)
	if hour > 0 {
		for {
			if err := clickBtnByPath(page, "#cb7-schedweek233 > tbody > tr:nth-child(1) > td > table > tbody > tr > td:nth-child(3) > button:nth-child(4)"); err != nil {
				log.Fatalln(err)
			}

			screenDate, err = checkScreenDate(page, "#cb7-schedweek-date-title233 > b")
			if err != nil {
				return err
			}
			targetY, targetM, targetD = extractDate(screenDate)
			targetObj := createTimeDate(targetY, targetM, targetD)
			hour = subtractHour(sentObj, targetObj)

			if hour == 0 {
				break
			}
		}
	} else if hour < 0 {
		for {
			if err := clickBtnByPath(page, "#cb7-schedweek233 > tbody > tr:nth-child(1) > td > table > tbody > tr > td:nth-child(3) > button:nth-child(2)"); err != nil {
				return err
			}

			screenDate, err = checkScreenDate(page, "#cb7-schedweek-date-title233 > b")
			if err != nil {
				return err
			}
			targetY, targetM, targetD = extractDate(screenDate)
			targetObj := createTimeDate(targetY, targetM, targetD)
			hour = subtractHour(sentObj, targetObj)

			if hour == 0 {
				break
			}
		}
	}
	return nil
}

// 日付の差分(hour)を返す
func subtractHour(day1 time.Time, day2 time.Time) int {
	subtract := day1.Sub(day2)
	return int(subtract.Hours())
}

func createTimeDate(year string, month string, day string) time.Time {
	location, err := time.LoadLocation("Asia/Tokyo")
	if err != nil {
		log.Fatalln(err)
	}

	y, _ := strconv.Atoi(year)
	m := checkMonth(month)
	d, _ := strconv.Atoi(day)
	timeObj := time.Date(y, m, d, 0, 0, 0, 0, location)

	return timeObj
}

func checkMonth(month string) time.Month {
	var t time.Month
	switch month {
	case "1", "01":
		t = time.January
	case "2", "02":
		t = time.February
	case "3", "03":
		t = time.March
	case "4", "04":
		t = time.April
	case "5", "05":
		t = time.May
	case "6", "06":
		t = time.June
	case "7", "07":
		t = time.July
	case "8", "08":
		t = time.August
	case "9", "09":
		t = time.September
	case "10":
		t = time.October
	case "11":
		t = time.November
	case "12":
		t = time.December
	}
	return t
}

// 画面に表示されている日付を返す
func checkScreenDate(page *agouti.Page, selector string) (string, error) {
	screenDate := page.Find(selector)
	if _, err := screenDate.Count(); err != nil {
		return "", err
	}
	date, _ := screenDate.Text()
	return date, nil
}

// 文字列から日付を抽出する
func extractDate(str string) (string, string, string) {
	var num, year, month, day string
	for _, c := range str {
		flag := checkRegexp(`[0-9]`, string(c))
		if flag == true {
			num = num + string(c)
		} else {
			switch string(c) {
			case "年":
				year = num
				num = ""
			case "月":
				month = num
				num = ""
			case "日":
				day = num
				num = ""
			}
		}
		if day != "" {
			break
		}
	}
	return year, month, day
}

// 文字列から期間を抽出する
func extractPeriod(str string) string {
	var period string
	for _, c := range str {
		flag := checkRegexp(`[0-9]`, string(c))
		if flag == true {
			period = period + string(c)
		} else {
			switch string(c) {
			case ":", "：":
				period = period + ":"
			case "-", "ー", "~", "〜":
				period = period + "-"
			default:
				period = period + "/"
			}
		}
	}
	return period
}

// 正規表現
func checkRegexp(reg, str string) bool {
	r := regexp.MustCompile(reg).Match([]byte(str))
	return r
}

func booking(page *agouti.Page, date string, start string, end string, title string, room string) error {
	reserveButton := page.FindByXPath("//*[@id=\"content-wrapper\"]/div[4]/div/div[1]/table/tbody/tr/td[1]/table/tbody/tr/td[1]/span/span/a")
	_, err := reserveButton.Count()
	if err != nil {
		return err
	}
	reserveButton.Click()

	// set the date
	yearDom := getPageDOM(page).Find("select[name='SetDate.Year']").Children()
	monthDom := getPageDOM(page).Find("select[name='SetDate.Month']").Children()
	dayDom := getPageDOM(page).Find("select[name='SetDate.Day']").Children()
	startHourDom := getPageDOM(page).Find("select[name='SetTime.Hour']").Children()
	startMinuteDom := getPageDOM(page).Find("select[name='SetTime.Minute']").Children()
	endHourDom := getPageDOM(page).Find("select[name='EndTime.Hour']").Children()
	endMinuteDom := getPageDOM(page).Find("select[name='EndTime.Minute']").Children()

	years := make([]string, yearDom.Length())
	months := make([]string, monthDom.Length())
	days := make([]string, dayDom.Length())
	startHours := make([]string, startHourDom.Length())
	startMinutes := make([]string, startMinuteDom.Length())
	endHours := make([]string, endHourDom.Length())
	endMinutes := make([]string, endMinuteDom.Length())

	yearDom.Each(func(i int, g *goquery.Selection) {
		tx := g.Text()
		years[i] = tx
	})
	monthDom.Each(func(i int, g *goquery.Selection) {
		tx := g.Text()
		months[i] = tx
	})
	dayDom.Each(func(i int, g *goquery.Selection) {
		tx := g.Text()
		days[i] = tx
	})
	startHourDom.Each(func(i int, g *goquery.Selection) {
		tx := g.Text()
		startHours[i] = tx
	})
	startMinuteDom.Each(func(i int, g *goquery.Selection) {
		tx := g.Text()
		startMinutes[i] = tx
	})
	endHourDom.Each(func(i int, g *goquery.Selection) {
		tx := g.Text()
		endHours[i] = tx
	})
	endMinuteDom.Each(func(i int, g *goquery.Selection) {
		tx := g.Text()
		endMinutes[i] = tx
	})

	dateSplit := strings.Split(date, "/")
	yearIndex, err := searchIndex(years, dateSplit[0])
	if err != nil {
		return err
	}
	monthIndex, err := searchIndex(months, dateSplit[1])
	if err != nil {
		return err
	}
	dayIndex, err := searchIndex(days, dateSplit[2])
	if err != nil {
		return err
	}

	startSplit := strings.Split(start, ":")
	endSplit := strings.Split(end, ":")
	startHourIndex, err := searchIndex(startHours, startSplit[0]+"時")
	if err != nil {
		return err
	}
	startMinuteIndex, err := searchIndex(startMinutes, startSplit[1]+"分")
	if err != nil {
		return err
	}
	endHourIndex, err := searchIndex(endHours, endSplit[0]+"時")
	if err != nil {
		return err
	}
	endMinuteIndex, err := searchIndex(endMinutes, endSplit[1]+"分")
	if err != nil {
		return err
	}

	yearX := page.FindByName("SetDate.Year")
	_, err = yearX.Count()
	if err != nil {
		return err
	}
	monthX := page.FindByName("SetDate.Month")
	_, err = monthX.Count()
	if err != nil {
		return err
	}
	dayX := page.FindByName("SetDate.Day")
	_, err = dayX.Count()
	if err != nil {
		return err
	}
	startHourX := page.FindByName("SetTime.Hour")
	_, err = startHourX.Count()
	if err != nil {
		return err
	}
	startMinuteX := page.FindByName("SetTime.Minute")
	_, err = startMinuteX.Count()
	if err != nil {
		return err
	}
	endHourX := page.FindByName("EndTime.Hour")
	_, err = endHourX.Count()
	if err != nil {
		return err
	}
	endMinuteX := page.FindByName("EndTime.Minute")
	_, err = endMinuteX.Count()
	if err != nil {
		return err
	}

	err = yearX.Select(years[yearIndex])
	if err != nil {
		return err
	}
	err = monthX.Select(months[monthIndex])
	if err != nil {
		return err
	}
	err = dayX.Select(days[dayIndex])
	if err != nil {
		return err
	}
	err = startHourX.Select(startHours[startHourIndex])
	if err != nil {
		return err
	}
	err = startMinuteX.Select(startMinutes[startMinuteIndex])
	if err != nil {
		return err
	}
	err = endHourX.Select(endHours[endHourIndex])
	if err != nil {
		return err
	}
	err = endMinuteX.Select(endMinutes[endMinuteIndex])
	if err != nil {
		return err
	}

	// set the title
	detail := page.FindByName("Detail")
	if _, err := detail.Count(); err != nil {
		return err
	}
	detail.Fill(title)

	// choose room
	xpath := ""
	switch room {
	case "第一会議室":
		xpath = "//*[@id=\"content-wrapper\"]/div[4]/div/form/div[2]/table/tbody/tr/td/table/tbody/tr[2]/td/div/div[1]/div/table/tbody/tr[7]/td/table/tbody/tr[1]/td[3]/select/option[1]"
	case "第二会議室":
		xpath = "//*[@id=\"content-wrapper\"]/div[4]/div/form/div[2]/table/tbody/tr/td/table/tbody/tr[2]/td/div/div[1]/div/table/tbody/tr[7]/td/table/tbody/tr[1]/td[3]/select/option[2]"
	case "打合せルーム":
		xpath = "//*[@id=\"content-wrapper\"]/div[4]/div/form/div[2]/table/tbody/tr/td/table/tbody/tr[2]/td/div/div[1]/div/table/tbody/tr[7]/td/table/tbody/tr[1]/td[3]/select/option[3]"
	}
	theRoomY := page.FindByXPath(xpath)
	theRoomY.Click()

	time.Sleep(2 * time.Second)

	// submit to make a reservation
	entryButton := page.FindByName("Entry")
	_, err = entryButton.Count()
	if err != nil {
		return err
	}
	entryButton.Click()
	fmt.Println("Booking complete:", years[yearIndex], months[monthIndex], days[dayIndex], startHours[startHourIndex], startMinutes[startMinuteIndex], endHours[endHourIndex], endMinutes[endMinuteIndex])

	return nil
}

func Execute(year string, month string, day string, week string, start string, end string, people string, title string, room string) error {
	log.Println("Execute in cybozu is called:", year, month, day, week, start, end, people, title, room)
	// set of Chrome
	driver := agouti.ChromeDriver(agouti.Browser("chrome"))
	if err := driver.Start(); err != nil {
		return err
	}
	defer driver.Stop()

	page, err := driver.NewPage()
	if err != nil {
		return err
	}

	// sample Cybozu
	if err := page.Navigate(url); err != nil {
		return err
	}

	// login
	if err := login(page, loginName); err != nil {
		return err
	}

	// get group list
	groupsDom := getPageDOM(page).Find("select[name='GID']").Children()
	groups := make([]string, groupsDom.Length())
	groupsDom.Each(func(i int, sel *goquery.Selection) {
		groups[i] = sel.Text()
		// fmt.Println(i, groups[i])
	})

	// move to meeting room page
	group := page.FindByName("GID")
	if _, err := group.Count(); err != nil {
		fmt.Println("Cannot find path:", err)
	}
	group.Select(groups[9]) // "(全施設)"

	// ページ遷移後、次のコンテンツが表示されるまでにタイムラグが生じる
	time.Sleep(1 * time.Second)

	// make a reservation
	// date := "2019年/4月/23(火)"
	// start := "10:00"
	// end := "11:30"
	date := year + "年/" + month + "月/" + day + week
	if err := booking(page, date, start, end, title, room); err != nil {
		return err
	}

	time.Sleep(3 * time.Second)
	return nil
}

func Schedules(year string, month string, day string, start string, end string, people string) (map[string]Detail, error) {
	log.Println("Schedules in cybozu is called:", year, month, day, start, end, people)

	driver := agouti.ChromeDriver(agouti.Browser("chrome"))
	// driver := agouti.ChromeDriver(
	// 	agouti.ChromeOptions("args", []string{
	// 		"--headless",
	// 		"--disable-gpu",
	// 	}),
	// 	agouti.Debug,
	// )

	if err := driver.Start(); err != nil {
		return nil, err
	}
	defer driver.Stop()

	page, err := driver.NewPage()
	if err != nil {
		return nil, err
	}

	if err := page.Navigate(url); err != nil {
		return nil, err
	}

	if err := login(page, loginName); err != nil {
		return nil, err
	}

	log.Println("Compare Period")
	if err := comparePeriod(page, year, month, day); err != nil {
		log.Println("Compare Period Err", err)
		return nil, err
	}

	// get group list
	groupsDom := getPageDOM(page).Find("select[name='GID']").Children()
	groups := make([]string, groupsDom.Length())
	groupsDom.Each(func(i int, sel *goquery.Selection) {
		groups[i] = sel.Text()
		// fmt.Println(i, groups[i])
	})

	// move to meeting room page
	group := page.FindByName("GID")
	if _, err := group.Count(); err != nil {
		return nil, err
	}
	group.Select(groups[10]) // "会議室"

	// ページ遷移後、次のコンテンツが表示されるまでにタイムラグが生じる
	time.Sleep(1 * time.Second)

	dom := getPageDOM(page).Find("tr[class='eventrow']").Children()
	rooms := make(map[string]Detail)
	var roomname string
	var detail Detail
	dom.Each(func(i int, s *goquery.Selection) {
		switch i % 8 {
		case 0:
			roomname = s.Find("a").First().Text()
			detail.Memo = s.Find("span[class='facilitymemo']").First().Text()
		case 1:
			str := s.Find("span[class='eventDateTime']").Text()
			period := extractPeriod(str)
			detail.OccupiedTime = period
		default:
			rooms[roomname] = detail
		}
	})

	for k, v := range rooms {
		log.Printf("k:%v, memo:%v, occupied:%v", k, v.Memo, v.OccupiedTime)
	}

	return rooms, nil
}

func Cancel(year string, month string, day string, title string) error {
	log.Println("Cancel in cybozu is called:", year, month, day, title)

	driver := agouti.ChromeDriver(agouti.Browser("chrome"))
	if err := driver.Start(); err != nil {
		return err
	}
	defer driver.Stop()

	page, err := driver.NewPage()
	if err != nil {
		return err
	}

	if err := page.Navigate(url); err != nil {
		return err
	}

	if err := login(page, loginName); err != nil {
		return err
	}

	if err := comparePeriod(page, year, month, day); err != nil {
		return err
	}

	var href string
	dom := getPageDOM(page).Find("#cb7-schedweek-view233 > table > tbody > tr.eventrow > td:nth-child(2)").Find("a")
	dom.EachWithBreak(func(i int, s *goquery.Selection) bool {
		if flag := strings.Contains(s.Text(), title); flag == true {
			href, _ = s.Attr("href")
			return false
		}
		return true
	})

	selector := "a[href='" + href + "']"
	if err := clickBtnByPath(page, selector); err != nil {
		return err
	}

	if err := clickBtnByPath(page, "#content-wrapper > div.content > div > div.menubar > table > tbody > tr > td:nth-child(1) > span:nth-child(2) > a"); err != nil {
		return err
	}

	if err := clickBtnByPath(page, "#content-wrapper > div.content > div > form > table > tbody > tr > td > table > tbody > tr:nth-child(2) > td > div > div.vr_formCommitWrapper > p > input:nth-child(1)"); err != nil {
		return err
	}

	return nil
}

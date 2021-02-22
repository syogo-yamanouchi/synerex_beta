package desknets // import "github.com/synerex/meeting_desknets"

import (
	"errors"
	"log"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/PuerkitoBio/goquery"

	"github.com/sclevine/agouti"
)

var (
	url       = "https://trial.desknets.com/cgi-bin/dneo/dneo.cgi?"
	loginName = "岡田陽太"
)

func getPageDOM(page *agouti.Page) *goquery.Document {
	// get whole page
	wholePage, err := page.HTML()
	if err != nil {
		log.Fatalln(err)
	}

	// goquery
	readerOfPage := strings.NewReader(wholePage)
	pageDom, err := goquery.NewDocumentFromReader(readerOfPage)
	if err != nil {
		log.Fatalln(err)
	}
	return pageDom
}

func getSelectBoxByClass(page *agouti.Page, class string) ([]string, error) {
	selector := "select[class='" + class + "']"
	dom := getPageDOM(page).Find(selector).First().Children()
	options := make([]string, dom.Length())
	dom.Each(func(i int, s *goquery.Selection) {
		options[i] = s.Text()
	})
	return options, nil
}

func getSelectBoxByName(page *agouti.Page, name string) ([]string, error) {
	selector := "select[name='" + name + "']"
	dom := getPageDOM(page).Find(selector).First().Children()
	options := make([]string, dom.Length())
	dom.Each(func(i int, s *goquery.Selection) {
		options[i] = s.Text()
	})
	return options, nil
}

func searchIndex(data []string, target string) (int, error) {
	index := -1
	for i, v := range data {
		if v == target {
			index = i
		}
	}
	if index == -1 {
		errMsg := "failed to set parameter: " + target
		return index, errors.New(errMsg)
	}
	return index, nil
}

func login(page *agouti.Page, user string) error {
	// get user list
	users, err := getSelectBoxByName(page, "uid")
	if err != nil {
		return err
	}

	// search index
	userIndex, err := searchIndex(users, user)
	if err != nil {
		return err
	}

	// set login user
	name := page.FindByName("uid")
	if _, err := name.Count(); err != nil {
		return err
	}
	name.Select(users[userIndex])

	// search submit button
	submitBtn := page.FindByID("login-btn")
	if _, err := submitBtn.Count(); err != nil {
		return err
	}

	// click
	if err := submitBtn.Click(); err != nil {
		return err
	}

	return nil
}

func comparePeriod(page *agouti.Page, year string, month string, day string) error {
	screenDate, err := checkScreenDate(page, "#jsch-schweekgrp > form > div.co-actionwrap.top > div.jsch-cal-date-header.sch-cal-date-header > span.cal-date.sch-term-text > span:nth-child(1)")
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
			if err := clickBtnByPath(page, "#jsch-schweekgrp > form > div.co-actionwrap.top > div.jsch-cal-date-header.sch-cal-date-header > span.cal-switch > a.co-ymd-next"); err != nil {
				log.Fatalln(err)
			}

			screenDate, err = checkScreenDate(page, "#jsch-schweekgrp > form > div.co-actionwrap.top > div.jsch-cal-date-header.sch-cal-date-header > span.cal-date.sch-term-text > span:nth-child(1)")
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
			if err := clickBtnByPath(page, "#jsch-schweekgrp > form > div.co-actionwrap.top > div.jsch-cal-date-header.sch-cal-date-header > span.cal-switch > a.co-ymd-prev"); err != nil {
				return err
			}

			screenDate, err = checkScreenDate(page, "#jsch-schweekgrp > form > div.co-actionwrap.top > div.jsch-cal-date-header.sch-cal-date-header > span.cal-date.sch-term-text > span:nth-child(1)")
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

// 日付の差分(hour)を返す
func subtractHour(day1 time.Time, day2 time.Time) int {
	subtract := day1.Sub(day2)
	return int(subtract.Hours())
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

// 正規表現
func checkRegexp(reg, str string) bool {
	r := regexp.MustCompile(reg).Match([]byte(str))
	return r
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

func clickBtnByID(page *agouti.Page, id string) error {
	btn := page.FindByID(id)
	if _, err := btn.Count(); err != nil {
		return err
	}
	btn.Click()
	time.Sleep(1 * time.Second)
	return nil
}

func clickBtnByClass(page *agouti.Page, class string) error {
	btn := page.FirstByClass(class)
	if _, err := btn.Count(); err != nil {
		return err
	}
	btn.Click()
	time.Sleep(1 * time.Second)
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

func doubleClickByPath(page *agouti.Page, path string) error {
	btn := page.Find(path)
	if _, err := btn.Count(); err != nil {
		return err
	}
	btn.DoubleClick()
	time.Sleep(1 * time.Second)
	return nil
}

func setDate(page *agouti.Page, startHour string, startMinute string, endHour string, endMinute string) error {
	// get start hour list
	startHours, err := getSelectBoxByClass(page, "co-timepicker-hour")
	if err != nil {
		log.Fatalln(err)
	}

	// search index
	startHourIdx, err := searchIndex(startHours, startHour)
	if err != nil {
		return err
	}

	// set start hour
	strtH := page.FirstByClass("co-timepicker-hour")
	if _, err := strtH.Count(); err != nil {
		return err
	}
	strtH.Select(startHours[startHourIdx])

	// get start minute list
	startMinutes, err := getSelectBoxByClass(page, "co-timepicker-minute")
	if err != nil {
		log.Fatalln(err)
	}

	// search index
	startMinuteIdx, err := searchIndex(startMinutes, startMinute)
	if err != nil {
		log.Println(err)
	}

	// set start minute
	strtM := page.FirstByClass("co-timepicker-minute")
	if _, err := strtM.Count(); err != nil {
		return err
	}
	strtM.Select(startMinutes[startMinuteIdx])

	// get end hour list
	// Desknetsの仕様上、startHourと同じクラスが指定されていて関数を利用できない
	dom := getPageDOM(page).Find("select[class='co-timepicker-hour']").Last().Children()
	endHours := make([]string, dom.Length())
	dom.Each(func(i int, s *goquery.Selection) {
		endHours[i] = s.Text()
	})

	// search index
	endHourIdx, err := searchIndex(endHours, endHour)
	if err != nil {
		log.Fatalln(err)
	}

	// set end hour
	endH := page.AllByClass("co-timepicker-hour").At(1)
	if _, err := endH.Count(); err != nil {
		return err
	}
	endH.Select(endHours[endHourIdx])

	// get end minute list
	// Desknetsの仕様上、startMinuteと同じクラスが指定されていて関数を利用できない
	dom = getPageDOM(page).Find("select[class='co-timepicker-minute']").Last().Children()
	endMinutes := make([]string, dom.Length())
	dom.Each(func(i int, s *goquery.Selection) {
		endMinutes[i] = s.Text()
	})

	// search index
	endMinuteIdx, err := searchIndex(endMinutes, endMinute)
	if err != nil {
		log.Fatalln(err)
	}

	// set end minute
	endM := page.AllByClass("co-timepicker-minute").At(1)
	if _, err := endM.Count(); err != nil {
		return err
	}
	endM.Select(endMinutes[endMinuteIdx])

	return nil
}

func getFacilities(page *agouti.Page) (map[string]string, error) {
	// click search button
	if err := clickBtnByClass(page, "jsch-entry-plant-group-button"); err != nil {
		return nil, err
	}

	dom := getPageDOM(page).Find("div[class='sch-entry-plant-free-list']").Find("ul").Children()
	facilities := make(map[string]string)
	dom.Each(func(_ int, s *goquery.Selection) {
		name := s.Find("label").Text()
		pid, _ := s.Find("span").Attr("data-pid")
		facilities[name] = pid
	})

	return facilities, nil
}

func formatTime(colon string) (string, string) {
	slice := strings.Split(colon, ":")
	hour := slice[0] + "時"
	var minute string
	switch slice[1] {
	case "00":
		minute = "0分"
	case "30":
		minute = "30分"
	default:
		minute = "0分"
		log.Println("Failed to set the minute at formatTime function in desknets")
	}
	return hour, minute
}

func Schedule(year string, month string, day string, start string, end string, title string, room string) (map[string]string, error) {
	log.Println("Schedule in desknets is called:", year, month, day, start, end, title, room)

	driver := agouti.ChromeDriver(agouti.Browser("chrome"))
	if err := driver.Start(); err != nil {
		log.Fatalln(err)
	}
	defer driver.Stop()

	page, err := driver.NewPage()
	if err != nil {
		log.Fatalln(err)
	}

	if err := page.Navigate(url); err != nil {
		log.Fatalln(err)
	}

	// login
	if err := login(page, loginName); err != nil {
		return nil, err
	}
	time.Sleep(3 * time.Second) // Desknetsの仕様上、ログイン後、新しくコンテンツが読み込まれるまでに時間がかかる

	// click schedule button
	if err := clickBtnByPath(page, "#portal-content-1000 > div.portal-content-body > ul > li:nth-child(1)"); err != nil {
		return nil, err
	}

	// 送信された日付の画面を表示させる
	// date := "2020年03月16日(月)"
	if err := comparePeriod(page, year, month, day); err != nil {
		return nil, err
	}

	// move to the booking page
	if err := doubleClickByPath(page, "#jsch-schweekgrp > form > div.sch-gweek.sch-cal-group-week.jsch-cal-list.jco-print-template.sch-data-view-area > div.sch-gcal-target.me.cal-h-cell.jsch-cal > div.cal-h-week > table > tbody > tr > td:nth-child(1)"); err != nil {
		return nil, err
	}

	// optimize time format
	startHour, startMinute := formatTime(start)
	endHour, endMinute := formatTime(end)

	// set start and end datetime
	if err := setDate(page, startHour, startMinute, endHour, endMinute); err != nil {
		return nil, err
	}

	// click facility button
	if err := clickBtnByPath(page, "#inputfrm > div.co-ebtnarea.sch-ebtnarea > a:nth-child(1)"); err != nil {
		return nil, err
	}

	// 空いている施設一覧を取得する
	facilities, err := getFacilities(page)
	if err != nil {
		return nil, err
	}

	return facilities, nil
}

func getPID(data map[string]string, target string) string {
	for k, v := range data {
		if k == target {
			return v
		}
	}
	return ""
}

func Execute(year string, month string, day string, start string, end string, title string, room string) error {
	log.Println("Execute in desknets is called:", year, month, day, start, end, title, room)

	driver := agouti.ChromeDriver(agouti.Browser("chrome"))
	if err := driver.Start(); err != nil {
		log.Fatalln(err)
	}
	defer driver.Stop()

	page, err := driver.NewPage()
	if err != nil {
		log.Fatalln(err)
	}

	if err := page.Navigate(url); err != nil {
		log.Fatalln(err)
	}

	// login
	if err := login(page, loginName); err != nil {
		return err
	}
	time.Sleep(3 * time.Second) // Desknetsの仕様上、ログイン後、新しくコンテンツが読み込まれるまでに時間がかかる

	facilities, err := Schedule(year, month, day, start, end, title, room)
	if err != nil {
		return err
	}

	// check the checkbox
	pid := getPID(facilities, room)
	selector := "input[value='" + pid + "']"
	checkBx := page.Find(selector)
	if _, err := checkBx.Count(); err != nil {
		return err
	}
	checkBx.Check()

	// click OK button
	if err := clickBtnByPath(page, "#neodialog-sch-edit-plant > div.ui-dialog-buttonpane.ui-widget-content.ui-helper-clearfix > div > button:nth-child(1)"); err != nil {
		return err
	}

	// set title
	detail := page.FindByName("detail")
	if _, err := detail.Count(); err != nil {
		log.Fatalln(err)
	}
	detail.Fill(title)

	// submit
	if err := clickBtnByPath(page, "#inputfrm > div.co-actionwrap.top > div > input[type=submit]:nth-child(1)"); err != nil {
		log.Fatalln(err)
	}

	return nil
}

func Cancel(year string, month string, day string, title string) error {
	log.Println("Cancel in desknets is called:", year, month, day, title)

	driver := agouti.ChromeDriver(agouti.Browser("chrome"))
	if err := driver.Start(); err != nil {
		log.Fatalln(err)
	}
	defer driver.Stop()

	page, err := driver.NewPage()
	if err != nil {
		log.Fatalln(err)
	}

	if err := page.Navigate(url); err != nil {
		log.Fatalln(err)
	}

	// login
	if err := login(page, loginName); err != nil {
		return err
	}
	time.Sleep(3 * time.Second) // Desknetsの仕様上、ログイン後、新しくコンテンツが読み込まれるまでに時間がかかる

	// click schedule button
	if err := clickBtnByPath(page, "#portal-content-1000 > div.portal-content-body > ul > li:nth-child(1)"); err != nil {
		return err
	}

	// 送信された日付の画面を表示させる
	// date := "2020年03月16日(月)"
	if err := comparePeriod(page, year, month, day); err != nil {
		return err
	}

	// titleからキャンセルする予定を探し、リンクを取得する
	var href string
	dom := getPageDOM(page).Find("div[class='cal-item-box ui-draggable']").Find("a")
	dom.EachWithBreak(func(i int, s *goquery.Selection) bool {
		if flag := strings.Contains(s.Text(), title); flag == true {
			href, _ = s.Attr("href")
			return false
		}
		return true
	})

	// キャンセルする予定の詳細に遷移
	selector := "a[href='" + href + "']"
	if err := doubleClickByPath(page, selector); err != nil {
		return err
	}

	// 予定削除ボタンをクリック
	if err := clickBtnByPath(page, "#inputfrm > div.co-actionwrap.bottom > div > input.jco-input-del-submit"); err != nil {
		return err
	}

	// Confirm Delete
	if err := clickBtnByPath(page, "#neodialog-cal-delconfirm-dialog > div.ui-dialog-buttonpane.ui-widget-content.ui-helper-clearfix > div > button:nth-child(1)"); err != nil {
		return err
	}

	return nil
}

package selenium

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/PuerkitoBio/goquery"
	"github.com/sclevine/agouti"
)

func getPageDOM(page *agouti.Page) *goquery.Document {
	wholePage, err := page.HTML()
	if err != nil {
		fmt.Println("Failed to get whole page:", err)
	}
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
	}
	if index == -1 {
		errMsg := "Failed to set parameter: " + target
		return -1, errors.New(errMsg)
	} else {
		return index, nil
	}
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

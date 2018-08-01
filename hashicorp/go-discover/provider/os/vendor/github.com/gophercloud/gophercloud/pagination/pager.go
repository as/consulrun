package pagination

import (
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"strings"

	"github.com/gophercloud/gophercloud"
)

var (
	ErrPageNotAvailable = errors.New("The requested page does not exist.")
)

type Page interface {
	NextPageURL() (string, error)

	IsEmpty() (bool, error)

	GetBody() interface{}
}

type Pager struct {
	client *gophercloud.ServiceClient

	initialURL string

	createPage func(r PageResult) Page

	Err error

	Headers map[string]string
}

func NewPager(client *gophercloud.ServiceClient, initialURL string, createPage func(r PageResult) Page) Pager {
	return Pager{
		client:     client,
		initialURL: initialURL,
		createPage: createPage,
	}
}

func (p Pager) WithPageCreator(createPage func(r PageResult) Page) Pager {
	return Pager{
		client:     p.client,
		initialURL: p.initialURL,
		createPage: createPage,
	}
}

func (p Pager) fetchNextPage(url string) (Page, error) {
	resp, err := Request(p.client, p.Headers, url)
	if err != nil {
		return nil, err
	}

	remembered, err := PageResultFrom(resp)
	if err != nil {
		return nil, err
	}

	return p.createPage(remembered), nil
}

func (p Pager) EachPage(handler func(Page) (bool, error)) error {
	if p.Err != nil {
		return p.Err
	}
	currentURL := p.initialURL
	for {
		currentPage, err := p.fetchNextPage(currentURL)
		if err != nil {
			return err
		}

		empty, err := currentPage.IsEmpty()
		if err != nil {
			return err
		}
		if empty {
			return nil
		}

		ok, err := handler(currentPage)
		if err != nil {
			return err
		}
		if !ok {
			return nil
		}

		currentURL, err = currentPage.NextPageURL()
		if err != nil {
			return err
		}
		if currentURL == "" {
			return nil
		}
	}
}

func (p Pager) AllPages() (Page, error) {

	var pagesSlice []interface{}

	var body reflect.Value

	testPage, err := p.fetchNextPage(p.initialURL)
	if err != nil {
		return nil, err
	}

	pageType := reflect.TypeOf(testPage)

	if _, found := pageType.FieldByName("SinglePageBase"); found {
		return testPage, nil
	}

	switch pb := testPage.GetBody().(type) {
	case map[string]interface{}:

		var key string

		err = p.EachPage(func(page Page) (bool, error) {
			b := page.GetBody().(map[string]interface{})
			for k, v := range b {

				if !strings.HasSuffix(k, "links") {

					switch vt := v.(type) {
					case []interface{}:
						key = k
						pagesSlice = append(pagesSlice, vt...)
					}
				}
			}
			return true, nil
		})
		if err != nil {
			return nil, err
		}

		body = reflect.MakeMap(reflect.MapOf(reflect.TypeOf(key), reflect.TypeOf(pagesSlice)))
		body.SetMapIndex(reflect.ValueOf(key), reflect.ValueOf(pagesSlice))
	case []byte:

		err = p.EachPage(func(page Page) (bool, error) {
			b := page.GetBody().([]byte)
			pagesSlice = append(pagesSlice, b)

			pagesSlice = append(pagesSlice, []byte{10})
			return true, nil
		})
		if err != nil {
			return nil, err
		}
		if len(pagesSlice) > 0 {

			pagesSlice = pagesSlice[:len(pagesSlice)-1]
		}
		var b []byte

		for _, slice := range pagesSlice {
			b = append(b, slice.([]byte)...)
		}

		body = reflect.New(reflect.TypeOf(b)).Elem()
		body.SetBytes(b)
	case []interface{}:

		err = p.EachPage(func(page Page) (bool, error) {
			b := page.GetBody().([]interface{})
			pagesSlice = append(pagesSlice, b...)
			return true, nil
		})
		if err != nil {
			return nil, err
		}

		body = reflect.MakeSlice(reflect.TypeOf(pagesSlice), len(pagesSlice), len(pagesSlice))
		for i, s := range pagesSlice {
			body.Index(i).Set(reflect.ValueOf(s))
		}
	default:
		err := gophercloud.ErrUnexpectedType{}
		err.Expected = "map[string]interface{}/[]byte/[]interface{}"
		err.Actual = fmt.Sprintf("%T", pb)
		return nil, err
	}

	page := reflect.New(pageType)

	page.Elem().FieldByName("Body").Set(body)

	h := make(http.Header)
	for k, v := range p.Headers {
		h.Add(k, v)
	}
	page.Elem().FieldByName("Header").Set(reflect.ValueOf(h))

	return page.Elem().Interface().(Page), err
}

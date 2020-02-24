package wsc

import (
	"bytes"
	"io/ioutil"
	"mime/multipart"
	"net/http"
	"net/url"
	"time"
)

var httpClient *http.Client

// WebServiceClient defines a connection to a HTTP server
type WebServiceClient struct {
	URL      string
	Username string
	Password string
}

// Get sends a properly formatted GET request to a GlobalNOC Web Service
func (wsc *WebServiceClient) Get(method string, params map[string]string) (responseCode int, responseText string, err error) {
	baseURL, err := url.Parse(wsc.URL)

	if err != nil {
		return 0, "", err
	}

	baseURL.RawQuery = encodeGetParams(method, params)

	request, err := http.NewRequest(http.MethodGet, baseURL.String(), nil)

	if err != nil {
		// very unlikely
		return 0, "", err
	}

	return wsc.handleRequest(request)
}

// Post sends a properly formatted POST request to a GlobalNOC Web Service
func (wsc *WebServiceClient) Post(method string, params map[string]string) (responseCode int, responseText string, err error) {
	// encode the body as multipart/form-data
	mimeType, body, err := encodePostParams(method, params)

	if err != nil {
		return 0, "", err
	}

	request, err := http.NewRequest(http.MethodPost, wsc.URL, bytes.NewReader(body))

	if err != nil {
		// very unlikely
		return 0, "", err
	}

	// we need explicit Content-Type for the multipart field boundary
	request.Header.Set("Content-Type", mimeType)

	return wsc.handleRequest(request)
}

func encodeGetParams(method string, params map[string]string) string {
	qs := url.Values{}

	qs.Add("method", method)

	for k, v := range params {
		qs.Add(k, v)
	}

	return qs.Encode()
}

func encodePostParams(method string, params map[string]string) (mimeType string, b []byte, err error) {
	buf := new(bytes.Buffer)
	writer := multipart.NewWriter(buf)

	if err := writer.WriteField("method", method); err != nil {
		// very unlikely
		return "", nil, err
	}

	for k, v := range params {
		if err := writer.WriteField(k, v); err != nil {
			// very unlikely
			return "", nil, err
		}
	}

	if err := writer.Close(); err != nil {
		// very unlikely
		return "", nil, err
	}

	return writer.FormDataContentType(), buf.Bytes(), nil
}

func (wsc *WebServiceClient) handleRequest(request *http.Request) (int, string, error) {
	if wsc.Username != "" && wsc.Password != "" {
		request.SetBasicAuth(wsc.Username, wsc.Password)
	}

	response, err := httpClient.Do(request)

	if err != nil {
		return 0, "", err
	}

	// if we do not close the Body, we cannot reuse the http connection
	defer response.Body.Close()

	bytes, err := ioutil.ReadAll(response.Body)

	if err != nil {
		// this will probably be a network error
		return 0, "", err
	}

	return response.StatusCode, string(bytes), nil
}

func init() {
	httpClient = &http.Client{
		Timeout: time.Second * 15,
	}
}

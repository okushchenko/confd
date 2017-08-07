package rancher

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"net/http"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/mitchellh/mapstructure"
)

const (
	MetaDataURL = "http://rancher-metadata"
)

type Client struct {
	url        string
	httpClient *http.Client
}

func (c *Client) Configure(configRaw map[string]string) error {
	var config Config
	if err := mapstructure.Decode(configRaw, &config); err != nil {
		return err
	}

	url := MetaDataURL
	if len(strings.Split(config.BackendNodes, ",")) > 0 {
		url = "http://" + strings.Split(config.BackendNodes, ",")[0]
	}

	log.Printf("Using Rancher Metadata URL: " + url)
	c.url = url
	c.httpClient = &http.Client{}
	return c.testConnection()
}

func (c *Client) GetValues(keys []string) (map[string]string, error) {
	vars := map[string]string{}

	for _, key := range keys {
		body, err := c.makeMetaDataRequest(key)
		if err != nil {
			return vars, err
		}

		var jsonResponse interface{}
		if err = json.Unmarshal(body, &jsonResponse); err != nil {
			return vars, err
		}

		if err = treeWalk(key, jsonResponse, vars); err != nil {
			return vars, err
		}
	}
	return vars, nil
}

func treeWalk(root string, val interface{}, vars map[string]string) error {
	switch val.(type) {
	case map[string]interface{}:
		for k := range val.(map[string]interface{}) {
			treeWalk(strings.Join([]string{root, k}, "/"), val.(map[string]interface{})[k], vars)
		}
	case []interface{}:
		for i, item := range val.([]interface{}) {
			idx := strconv.Itoa(i)
			if i, isMap := item.(map[string]interface{}); isMap {
				if name, exists := i["name"]; exists {
					idx = name.(string)
				}
			}

			treeWalk(strings.Join([]string{root, idx}, "/"), item, vars)
		}
	case bool:
		vars[root] = strconv.FormatBool(val.(bool))
	case string:
		vars[root] = val.(string)
	case float64:
		vars[root] = strconv.FormatFloat(val.(float64), 'f', -1, 64)
	case nil:
		vars[root] = "null"
	default:
		log.Printf("Unknown type: " + reflect.TypeOf(val).Name())
	}
	return nil
}

func (c *Client) makeMetaDataRequest(path string) ([]byte, error) {
	req, _ := http.NewRequest("GET", strings.Join([]string{c.url, path}, ""), nil)
	req.Header.Set("Accept", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	return ioutil.ReadAll(resp.Body)
}

func (c *Client) testConnection() error {
	var err error
	maxTime := 20 * time.Second

	for i := 1 * time.Second; i < maxTime; i *= time.Duration(2) {
		if _, err = c.makeMetaDataRequest("/"); err != nil {
			time.Sleep(i)
		} else {
			return nil
		}
	}
	return err
}

func (c *Client) WatchPrefix(prefix string, keys []string, waitIndex uint64) (uint64, error) {
	// Watches are not implemented in Rancher Metadata Service
	return 0, nil
}

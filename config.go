package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
)

func getConnectionString() (string, error) {
	config, err := _getConfig()
	if err != nil {
		return "", fmt.Errorf("getConfig() -> %v", err)
	}

	return config["connection_string"].(string), nil
}

func _getConfig() (map[string]interface{}, error) {
	jsonFile, err := os.Open("config.json")
	if err != nil {
		return nil, fmt.Errorf("os.Open(\"config.json\") -> %v", err)
	}
	defer func(jsonFile *os.File) {
		err := jsonFile.Close()
		if err != nil {
			log.Printf("An error occurred while closing config.json: %v", err)
		}
		log.Println("Closed config.json.")
	}(jsonFile)

	byteValue, err := ioutil.ReadAll(jsonFile)
	if err != nil {
		return nil, fmt.Errorf("ioutil.ReadAll(jsonFile: <os.File: config.json>) -> %v", err)
	}

	var result map[string]interface{}
	err = json.Unmarshal(byteValue, &result)
	if err != nil {
		return nil, fmt.Errorf("json.Unmarshal(data: ..., v: ...) -> %v", err)
	}

	return result, nil
}

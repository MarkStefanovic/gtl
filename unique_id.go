package main

import (
	"strings"

	"github.com/google/uuid"
)

func createUUID() string {
	uuidWithHyphen := uuid.New()
	return strings.Replace(uuidWithHyphen.String(), "-", "", -1)
}

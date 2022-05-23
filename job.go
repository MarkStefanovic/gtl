package main

type Job struct {
	BatchId string
	Id      string
	Name    string
	Cmd     string
	CmdArgs string
	SQL     string
	Retries int
}

package main

type User struct {
	//AllianceID            int64  `json:"allianceID"`
	CID  string `json:"CountryID"`
	HqLv int    `json:"HqLevel"`
	ID   int64  `json:"id"`
	Lv   int    `json:"level"`
	RAT  int64  `json:"realActivityTimestamp"`
}

package str2duration

import (
	"errors"
	"regexp"
	"strconv"
	"strings"
	"time"
)

const (
	hoursByDay  = 24  //hours in a day
	hoursByWeek = 168 //hours in a weekend
)

/*
DisableCheck speed up performance disabling aditional checks
in the input string. If DisableCheck is true then when input string is
is invalid the time.Duration returned is always 0s and err is always nil.
By default DisableCheck is false.
*/
var DisableCheck bool
var reTimeDecimal *regexp.Regexp
var reDuration *regexp.Regexp

func init() {
	reTimeDecimal = regexp.MustCompile(`(?i)(\d+)(?:(?:\.)(\d+))?((?:[mµn])?s)$`)
	reDuration = regexp.MustCompile(`(?i)^(?:(\d+)(?:w))?(?:(\d+)(?:d))?(?:(\d+)(?:h))?(?:(\d{1,2})(?:m))?(?:(\d+)(?:s))?(?:(\d+)(?:ms))?(?:(\d+)(?:(?:µ|u)s))?(?:(\d+)(?:ns))?$`)
}

//Str2Duration returns time.Duration from string input
func Str2Duration(str string) (time.Duration, error) {

	var err error
	/*
		Go time.Duration string can returns lower times like nano, micro and milli seconds in decimal
		format, for example, 1 second with 1 nano second is 1.000000001s. For this when a dot is in the
		string then that time is formatted in nanoseconds, this example returns 1000000001ns
	*/
	if strings.Contains(str, ".") {
		str, err = decimalTimeToNano(str)
		if err != nil {
			return time.Duration(0), err
		}
	}

	if !DisableCheck {
		if !reDuration.MatchString(str) {
			return time.Duration(0), errors.New("invalid input duration string")
		}
	}

	var du time.Duration

	//errors ignored because regex
	for _, match := range reDuration.FindAllStringSubmatch(str, -1) {

		//weeks
		if len(match[1]) > 0 {
			w, _ := strconv.Atoi(match[1])
			du += time.Duration(w*hoursByWeek) * time.Hour
		}

		//days
		if len(match[2]) > 0 {
			d, _ := strconv.Atoi(match[2])
			du += time.Duration(d*hoursByDay) * time.Hour
		}

		//hours
		if len(match[3]) > 0 {
			h, _ := strconv.Atoi(match[3])
			du += time.Duration(h) * time.Hour
		}

		//minutes
		if len(match[4]) > 0 {
			m, _ := strconv.Atoi(match[4])
			du += time.Duration(m) * time.Minute
		}

		//seconds
		if len(match[5]) > 0 {
			s, _ := strconv.Atoi(match[5])
			du += time.Duration(s) * time.Second
		}

		//milliseconds
		if len(match[6]) > 0 {
			ms, _ := strconv.Atoi(match[6])
			du += time.Duration(ms) * time.Millisecond
		}

		//microseconds
		if len(match[7]) > 0 {
			ms, _ := strconv.Atoi(match[7])
			du += time.Duration(ms) * time.Microsecond
		}

		//nanoseconds
		if len(match[8]) > 0 {
			ns, _ := strconv.Atoi(match[8])
			du += time.Duration(ns) * time.Nanosecond
		}
	}

	return du, nil
}

func decimalTimeToNano(str string) (string, error) {

	var dotPart, dotTime, dotTimeDecimal, dotUnit string

	if !DisableCheck {
		if !reTimeDecimal.MatchString(str) {
			return "", errors.New("invalid input duration string")
		}
	}

	var t = reTimeDecimal.FindAllStringSubmatch(str, -1)

	dotPart = t[0][0]
	dotTime = t[0][1]
	dotTimeDecimal = t[0][2]
	dotUnit = t[0][3]

	nanoSeconds := 1
	switch dotUnit {
	case "s":
		nanoSeconds = 1000000000
		dotTimeDecimal += strings.Repeat("0", 9-len(dotTimeDecimal))
	case "ms":
		nanoSeconds = 1000000
		dotTimeDecimal += strings.Repeat("0", 6-len(dotTimeDecimal))
	case "µs", "us":
		nanoSeconds = 1000
		dotTimeDecimal += strings.Repeat("0", 3-len(dotTimeDecimal))
	}

	//errors ignored because regex

	//timeMajor is the part decimal before point
	timeMajor, _ := strconv.Atoi(dotTime)
	timeMajor = timeMajor * nanoSeconds

	//timeMajor is the part in decimal after point
	timeMinor, _ := strconv.Atoi(dotTimeDecimal)

	newNanoTime := timeMajor + timeMinor

	return strings.Replace(str, dotPart, strconv.Itoa(newNanoTime)+"ns", 1), nil
}

package bottleneck_localization

import (
	"encoding/json"
	"fmt"
	"github.com/gorilla/mux"
	"github.com/sirupsen/logrus"
	"io/ioutil"
	"math"
	"net/http"
	"os"
	"reflect"
	"strconv"
)

type bottleneckInfo struct {
	bottleneck string
	delay float64
}

type topsisArgs struct {
	frequency float64
	delay float64
}

type Error struct {
	ErrorCode   string `json:"errType"`
	ErrorString string `json:"errString"`
}

type Result struct {
	ResultType string `json:"type"`
	ResultStr string `json:"result"`
}

type node struct {
	spanID string
	duration float64
	parents []string
	processID string
	processingTime float64
}

var floatType = reflect.TypeOf(float64(0))
var stringType = reflect.TypeOf("")
var log = logrus.New()

func getFloat(unk interface{}) (float64, error) {
	switch i := unk.(type) {
	case float64:
		return i, nil
	case float32:
		return float64(i), nil
	case int64:
		return float64(i), nil
	case int32:
		return float64(i), nil
	case int:
		return float64(i), nil
	case uint64:
		return float64(i), nil
	case uint32:
		return float64(i), nil
	case uint:
		return float64(i), nil
	case string:
		return strconv.ParseFloat(i, 64)
	default:
		v := reflect.ValueOf(unk)
		v = reflect.Indirect(v)
		if v.Type().ConvertibleTo(floatType) {
			fv := v.Convert(floatType)
			return fv.Float(), nil
		} else if v.Type().ConvertibleTo(stringType) {
			sv := v.Convert(stringType)
			s := sv.String()
			return strconv.ParseFloat(s, 64)
		} else {
			return math.NaN(), fmt.Errorf("Can't convert %v to float64", v.Type())
		}
	}
}

func aggregateResults(urlServiceMap map[string][]bottleneckInfo, w http.ResponseWriter) {
	finalBottleneck := ""
	result := Result{ResultType: "Bottleneck", ResultStr: ""}
	urlCounter := make(map[string]float64)
	urlServiceMapFinal := make(map[string]string)
	for key, value2 := range urlServiceMap {
		tempMap := make(map[string]topsisArgs)
		for _, element := range value2 {
			if value, ok := tempMap[element.bottleneck]; ok {
				tempDelay := ((value.delay*float64(value.frequency))+element.delay)/float64(value.frequency + 1)
				tempMap[element.bottleneck] = topsisArgs{frequency: value.frequency + 1, delay: tempDelay }
			} else {
				tempMap[element.bottleneck] = topsisArgs{frequency: 1, delay: element.delay}
			}
		}
		counter := float64(0)
		for _,value := range tempMap {
			counter += float64(value.frequency)
		}

		urlServiceMapFinal[key] = topsis(tempMap)
		urlCounter[key] = counter
	}

	//Log Results
	log.Info("Performing url Aggregation")

	tempMax := float64(0)
	for key,value := range urlCounter {
		log.Info("url = ", key, " total calls = ",value, " bottleneck = ", urlServiceMapFinal[key])
		if (value > tempMax) {
			tempMax = value
			finalBottleneck = urlServiceMapFinal[key]
		}
	}

	if (finalBottleneck != "") {
		result = Result{ResultType: "Bottleneck", ResultStr: finalBottleneck}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(result)
	}
}

func findBottlenecks(data []interface{}) map[string][]bottleneckInfo {
	urlServiceMap := make(map[string][]bottleneckInfo)
	spanNodes := []node{}
	messages := make(map[string]float32)
	for index,_ := range data {
		spanMap := make(map[string]node)
		var primaryUrl string
		serviceMap := make(map[string]string)
		trace := data[index].(map[string]interface{})
		traceID := trace["traceID"]
		spans := trace["spans"].([]interface{})
		log.Debug(	"traceID =",traceID)
		processes := trace["processes"].(map[string]interface{})

		//Populate Processes
		for key,value := range processes {
			serviceMap[key] = value.(map[string]interface{})["serviceName"].(string)
		}

		for index,_ := range spans {
			span := spans[index].(map[string]interface{})
			var parents []string
			references := span["references"].([]interface{})
			tags := span["tags"].([]interface{})

			for index,_ := range references {
				reference := references[index].(map[string]interface{})
				if (reference["refType"] == "CHILD_OF"){
					parents = append(parents, reference["spanID"].(string))
				}
			}

			if (len(parents) == 0) {
				for index,_ := range tags {
					tag := tags[index].(map[string]interface{})
					if (tag["key"] == "http.url"){
						primaryUrl = tag["value"].(string)
					}
				}
			}

			spanID := span["spanID"]
			processID := span["processID"]
			duration, _ := getFloat(span["duration"])
			spanNode := node{spanID: spanID.(string), processID: processID.(string), parents: parents,
				duration: duration, processingTime: duration}
			spanNodes = append(spanNodes, spanNode)
			spanMap[spanID.(string)] = spanNode
		}

		for _,value := range spanMap {
			if(len(value.parents) > 0){
				tempProcessingTime := spanMap[value.parents[0]].duration - value.duration
				tempSpanNode := node{spanID: spanMap[value.parents[0]].spanID, duration: spanMap[value.parents[0]].duration,
					parents: spanMap[value.parents[0]].parents, processID: spanMap[value.parents[0]].processID,
					processingTime: tempProcessingTime}
				spanMap[value.parents[0]] = tempSpanNode
				source := value.processID
				destination := spanMap[value.parents[0]].processID
				if (source != destination){
					key := serviceMap[destination] + ":" + serviceMap[source]
					if value, ok := messages[key]; ok {
						messages[key] = value + 1
					} else {
						messages[key] = 1
					}
				}
			}
		}

		bottleneckServiceID := ""
		tempMax := float64(0)
		for key,value := range spanMap {
			log.Debug("SpanID =", key,"Duration =", value.duration, "Processing Time =", value.processingTime, "ProcessID =", value.processID)
			if(value.processingTime > tempMax) {
				tempMax = value.processingTime
				bottleneckServiceID = value.processID
			}
		}

		//Populate URL Service Map
		if value, ok := urlServiceMap[primaryUrl]; ok {
			value = append(value, bottleneckInfo{bottleneck: serviceMap[bottleneckServiceID], delay: tempMax})
			urlServiceMap[primaryUrl] = value
		} else {
			var services []bottleneckInfo
			services = append(services, bottleneckInfo{bottleneck: serviceMap[bottleneckServiceID], delay: tempMax})
			urlServiceMap[primaryUrl] = services
		}

		log.Debug("Primary URL =",primaryUrl)
		log.Debug("Bottleneck MicroserviceID =",bottleneckServiceID, "and MicroserviceName =", serviceMap[bottleneckServiceID])

	}
	return urlServiceMap
}

func processTraces(w http.ResponseWriter, r *http.Request) {
	primaryMicroservice := r.FormValue("service")
	startTime := r.FormValue("startTime")
	endTime := r.FormValue("endTime")
	host := r.FormValue("host")
	var err2 Error
	log.WithFields(logrus.Fields{
		"pService": primaryMicroservice,
		"startTime":   startTime,
		"endTime": endTime,
		"host": host,
	}).Info(r.Host, r.URL)

	limit := "10000"
	url := "http://"+ host +":16686/api/traces?end="+ endTime + "&limit="+ limit +
		"&lookback=1h&maxDuration&minDuration&service="+ primaryMicroservice +"&start=" + startTime
	resp, err := http.Get(url)
	if err != nil {
		err2 = Error{ErrorCode: "204", ErrorString: " Error Response: "+ err.Error()}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(err2)
		log.Error("errCode:",err2.ErrorCode, " msg:", err2.ErrorString)
		return
	}
	defer resp.Body.Close()

	log.Info("Response status:", resp.Status)

	urlServiceMap := make(map[string][]bottleneckInfo)

	// Compute SpanNodes
	if resp.StatusCode == http.StatusOK {
		bodyBytes, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			err2 = Error{ErrorCode: "203", ErrorString: "Error Response: "+ err.Error()}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(err2)
			log.Error("errorCode:",err2.ErrorCode, " msg:", err2.ErrorString)
			return
		}
		bodyString := string(bodyBytes)
		var result map[string]interface{}

		json.Unmarshal([]byte(bodyString), &result)
		data := result["data"].([]interface{})

		if (len(data) == 0) {
			err2 = Error{ErrorCode: "201", ErrorString: "Incorrect Parameters"}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(err2)
			log.Error("errorCode:",err2.ErrorCode, " msg:", err2.ErrorString)
			return
		}
		urlServiceMap = findBottlenecks(data)
	} else {
		//Response Code Error
		err2 = Error{ErrorCode: "202", ErrorString: "Error Response Code:"+ string(resp.StatusCode)}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(err2)
		log.Error("errorCode:",err2.ErrorCode, " msg:", err2.ErrorString)
		return
	}
	aggregateResults(urlServiceMap, w)
}

func topsis(data map[string]topsisArgs) string {

	if (len(data) == 0) {
		log.Panic("Empty data to Topsis")
		panic("Empty data to Topsis")
	}

	finalScore := make(map[string]float64)
	bottleneck := ""
	freqWeight := float64(0.5)
	delayWeight := float64(0.5)
	tempCopy := data
	if (len(data) == 1) {
		for key,_ := range data {
			bottleneck = key
		}
	} else {
		//Implementation of Topsis for two input parameters
		//Step-1: Normalize and multiply weights
		freqNorm := float64(0)
		delayNorm := float64(0)

		for _,value := range data {
			freqNorm += float64(value.frequency * value.frequency)
			delayNorm += value.delay * value.delay
		}

		freqNorm = math.Sqrt(freqNorm)
		delayNorm = math.Sqrt(delayNorm)
		freqBest := float64(0)
		delayBest := float64(0)


		for key,value := range tempCopy {
			freq := (value.frequency/freqNorm)*freqWeight
			delay := (value.delay/delayNorm)*delayWeight
			tempCopy[key] = topsisArgs{frequency: freq,
				delay: delay }
			if (freq > freqBest) { freqBest = freq }
			if (delay > delayBest) { delayBest = delay}
		}

		freqWorst := freqBest
		delayWorst := delayBest

		for _, value := range tempCopy {
			if (value.frequency < freqWorst) {freqWorst = value.frequency}
			if (value.delay < delayWorst) {delayWorst = value.delay}
		}

		//Step-2: Compute Final Score
		for key,value := range tempCopy {
			positiveED := math.Sqrt((value.frequency - freqBest) * (value.frequency - freqBest) + (value.delay - delayBest) *
				(value.delay - delayBest))
			negetiveED := math.Sqrt((value.frequency - freqWorst) * (value.frequency - freqWorst) + (value.delay - delayWorst) *
				(value.delay - delayWorst))
			finalScore[key] = negetiveED / (positiveED + negetiveED)
		}
	}
	temp := float64(0)
	for key,value := range finalScore {
		if (value > temp) {
			temp = value
			bottleneck = key
		}
	}
	return bottleneck
}

func main() {
	//Initializing logging
	log.Formatter.(*logrus.TextFormatter).DisableColors = false
	log.Formatter.(*logrus.TextFormatter).DisableTimestamp = false
	log.Level = logrus.TraceLevel
	log.Out = os.Stdout

	lvl, ok := os.LookupEnv("LOG_LEVEL")
	// LOG_LEVEL not set, let's default to debug
	if !ok {
		lvl = "debug"
	}
	// parse string, this is built-in feature of logrus
	ll, err := logrus.ParseLevel(lvl)
	if err != nil {
		ll = logrus.DebugLevel
	}
	// set global log level
	log.SetLevel(ll)

	// Init router
	r := mux.NewRouter()

	// Route handles & endpoints
	r.HandleFunc("/getbottleneck", processTraces).Methods("GET")

	// Start server
	log.Fatal(http.ListenAndServe(":3000", r))
}
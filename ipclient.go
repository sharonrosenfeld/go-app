package main

import (
	"flag"
	"fmt"
	"os"
	"strconv"
	as "github.com/aerospike/aerospike-client-go"
	"bufio"
	"log"
        "math/rand"
	"net"
	"time"
	"strings"
)

var (
	host      string = "127.0.0.1"
	port      int    = 3000
	namespace string = "test"
	set       string = "ips"
	path      string = "none"
	x	  int = 2
	async     bool = false
	output_path string = "/tmp/output.txt"
	ips_map map[string]int
)

//this process reads X ips from file and then persist their frequency in a db
func main() {

	var err error

	// arguments
	flag.StringVar(&host, "host", host, "Remote host")
	flag.IntVar(&port, "port", port, "Remote port")
	flag.StringVar(&namespace, "namespace", namespace, "Namespace")
	flag.StringVar(&set, "set", set, "Set name")
	flag.StringVar(&path,"path",path, "file name" )
	flag.IntVar(&x,"x",x, "num ips to read" )
	flag.BoolVar(&async,"async",async,"is async mode")

	// parse flags
	flag.Parse()

	if (strings.Compare(path,"none") == 0){
		fmt.Println("pls insert input file : -path=< IN FILE >")
		return
	}else {
		fmt.Println("going to read ",x, "IPs","from ",path)
	}

	if async{
		fmt.Println("running in async mode" )
	}else {
		fmt.Println("running in sync mode")
	}

	//keep the ips frequncy processed only by this job
	ips_map = make(map[string]int)

	client, err := as.NewClient(host, port)
	panicOnError(err)
	policy := as.NewWritePolicy(0, 0)

	start := time.Now()

	//actual processing: reading the ips and persisting their freuency
	processIps(client,policy,async,x)

	fmt.Println("processing done =========")


	end := time.Now()
	elapsed := end.Sub(start)

	//create output file
	output, err := os.Create(output_path)
	if err != nil {
		log.Fatal(err)
	}
	defer output.Close()

	//write the process output to file
	for key, value := range ips_map {
		fmt.Println("Key:", key, "Value:", value)
		str1 := strconv.Itoa(value)
		output.WriteString(key+ " - "+str1 + "\n")
	}

	fmt.Println("operation took " + elapsed.String() + "\n")
	output.WriteString("operation took " + elapsed.String() + "\n")

	output.Sync()
}

func panicOnError(err error) {
	if err != nil {
		panic(err)
	}
}

//params: the aes client, async policy,channel (max) queue size
//async mode - ips are sent to other routing to be incremented at db
//sync mode - each ip is incremented right after its read
func processIps(client *as.Client,policy *as.WritePolicy,async bool,qSize int){

	//pass ips to other routine
	ips_channel := make(chan string , qSize)
	//signal to main thread that other routine is done
	done_channel := make(chan string, qSize)

	if (async){
		go asyncProcess(client,policy,ips_channel,done_channel)
	}

	i := 0
	//open input file
	file, err := os.Open(path)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	rand.Seed(time.Now().UnixNano())

	//for each ip - rand - if so, send to persistence
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		//need only X ips
		if (i < x){
			s := scanner.Text()
			log.Println("read ip ",s )
			j:= rand.Intn(2)
			if (j == 1){
				log.Println(" ip ",s, " was chosen" )
				//validate
				trial := net.ParseIP(s)
				if trial.To4() == nil {
					log.Println("%v is not an IPv4 address", trial)
					continue
				}

				ips_map[s]++
				if (async){
					//send to routine
					ips_channel<-s
				}else {
					incIp(client,policy,s);
				}
				i++
			}
		}else {
			break
		}

	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}

	log.Println("finished reading ips file")
	if (async){
		//send to routine a special sign to be processed - marking the last element
		ips_channel<-"done"
		//wait to signal from other routint
		<-done_channel
	}

}

//record key: ip , record value: count
func incIp(client *as.Client,policy *as.WritePolicy, ip string) {

	key, _ := as.NewKey(namespace, set, ip)
	bin := as.NewBin("count", 1)
	record, err := client.Operate(policy, key, as.AddOp(bin), as.GetOp())
	if err != nil {
		log.Fatal("error inc ",ip)
	}else {
		log.Println(" succesfuly updated record key:",record.Key,"value:",record.Bins["count"])
	}
}

//receive ips from channel and inc them in DB
func asyncProcess(client* as.Client,policy* as.WritePolicy, ips chan string, done chan string){
	for {
		ip := <-ips
		log.Println("received IP ",ip)
		if (strings.Compare(ip, "done") != 0){
			incIp(client,policy,ip)
		}else {
			done <- "done"
		}
	}
}



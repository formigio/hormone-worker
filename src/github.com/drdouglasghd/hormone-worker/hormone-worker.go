package main

import (
  "fmt"
  "log"
  "github.com/streadway/amqp"
  "os/exec"
  "bytes"
  "syscall"
  "encoding/json"
  "os"
  "io/ioutil"
  "strings"
)

func failOnError(err error, msg string) {
  if err != nil {
    log.Fatalf("%s: %s", msg, err)
    panic(fmt.Sprintf("%s: %s", msg, err))
  }
}

// Worker Config Received from Worker Registration Process
// {
//     "metadata": {
//         "code": "go",
//         "queue_type": "rabbitmq"
//     },
//     "config": {
//         "queue": "worker_go",
//         "type": "rabbitmq",
//         "endpoint": "xxxxxxx.rmq.cloudamqp.com",
//         "user": "xxxxxxx",
//         "vhost": "xxxxxxx",
//         "password": "xxxxxxxxxxxxxxxx",
//         "port": "5672"
//     }
// }

type Worker struct {
	Metadata struct {
		Code string `json:"code"`
		QueueType string `json:"queue_type"`
	} `json:"metadata"`
	Config struct {
		Queue string `json:"queue"`
		Type string `json:"type"`
		Endpoint string `json:"endpoint"`
		User string `json:"user"`
		Vhost string `json:"vhost"`
		Password string `json:"password"`
		Port string `json:"port"`
	} `json:"config"`
}

// $data = array(
//      "outcome"=>$customer->getId() . ' <-- New Magento Customer ID',
//      "error_code"=>$error,
//      "worker_code"=>$config->worker->code,
//      "queue"=>$queueConfig->queue,
//      "task"=>json_encode($task)
//  );
//  $data_string = json_encode($data);
type Outcome struct {
  Outcome string `json:"outcome"`
  ErrorCode string `json:"error_code"`
  WorkerCode string `json:"worker_code"`
  Queue string `json:"queue"`
  Task string `json:"task"`
  Pcb string `json:"pcb_path"`
}

// {
//     "code": "receive_message",
//     "object": "",
//     "done_when": "",
//     "documentation_source": "",
//     "description": "Receive Message from Rabbit",
//     "outcome_value_prompt": "receive_message",
//     "outcome_value_datatype": "string",
//     "responsible_party": "worker go",
//     "receive_signal": "hello_rabbit.start",
//     "payload": {
//         "buffer": {
//             "routine": "hello_rabbit",
//             "steps": {}
//         },
//         "pcb_path": "\/var\/www\/hormone\/tmp\/hormone\/objects\/hello_rabbit\/56cfef5633fa3.json"
//     },
//     "report_url": "http:\/\/www.hormone.dev\/hormone\/processes\/remotereport"
// }
type Task struct {
	Code string `json:"code"`
	Object string `json:"object"`
	DoneWhen string `json:"done_when"`
	DocumentationSource string `json:"documentation_source"`
	Description string `json:"description"`
	OutcomeValuePrompt string `json:"outcome_value_prompt"`
	OutcomeValueDatatype string `json:"outcome_value_datatype"`
	ResponsibleParty string `json:"responsible_party"`
	ReceiveSignal string `json:"receive_signal"`
	Payload struct {
		Buffer struct {
			Routine string `json:"routine"`
			Steps struct {
			} `json:"steps"`
		} `json:"buffer"`
		PcbPath string `json:"pcb_path"`
	} `json:"payload"`
}

func main() {

  // Read Config File
  configFile, err := os.Open("config.json")
  if err != nil {
      failOnError(err, "opening config file")
  }

  worker := Worker{}

  jsonParser := json.NewDecoder(configFile)
  if err = jsonParser.Decode(&worker); err != nil {
     failOnError(err, "parsing config file")
  }

  conn, err := amqp.Dial(fmt.Sprintf("amqp://%s:%s@%s:%s/%s",worker.Config.User,worker.Config.Password,worker.Config.Endpoint,worker.Config.Port,worker.Config.Vhost))
  failOnError(err, "Failed to connect to RabbitMQ")
  defer conn.Close()

  ch, err := conn.Channel()
  failOnError(err, "Failed to open a channel")
  defer ch.Close()

  q, err := ch.QueueDeclare(
    worker.Config.Queue, // name
    false,   // durable
    false,   // delete when usused
    false,   // exclusive
    false,   // no-wait
    nil,     // arguments
  )
  failOnError(err, "Failed to declare a queue")

  outq, err := ch.QueueDeclare(
    "worker_outcome", // name
    false,   // durable
    false,   // delete when usused
    false,   // exclusive
    false,   // no-wait
    nil,     // arguments
  )
  failOnError(err, "Failed to declare a queue")

  msgs, err := ch.Consume(
    q.Name, // queue
    "",     // consumer
    true,   // auto-ack
    false,  // exclusive
    false,  // no-local
    false,  // no-wait
    nil,    // args
  )
  failOnError(err, "Failed to register a consumer")

  forever := make(chan bool)

  go func() {
    for d := range msgs {
      log.Printf("Received a message: %s", d.Body)

      // No news is good news
      error := ""

      task := Task{}
      // payload := Payload{}

      err := json.Unmarshal([]byte(d.Body), &task)
      if err != nil {
          log.Printf("Task Parse: %v", err)
      }

      log.Printf("Task Payload - pcb: %s", task.Payload.PcbPath)

      // Write out payload for reference
      pcb :=  fmt.Sprintf("/tmp%s",task.Payload.PcbPath)
      log.Println(pcb)
      pyld, err := json.MarshalIndent(task.Payload,"","  ")
      if err != nil {
          log.Printf("Marshal Payload: %v", err)
      }
      err = ioutil.WriteFile(pcb, pyld, 0644)
      if err != nil {
          log.Printf("WriteFile: %v", err)
      }

      args := strings.Fields(task.OutcomeValuePrompt)
      cmdstr, args := args[0], args[1:]

      cmd := exec.Command(cmdstr,args...)

      // collect stdout and stderr
      var out bytes.Buffer
      cmd.Stdout = &out
      cmd.Stderr = &out

      // execute the command
        if err := cmd.Start(); err != nil {
          log.Printf("cmd.Start: %s", err)
        }

        // http://stackoverflow.com/questions/10385551/get-exit-code-go
        if err := cmd.Wait(); err != nil {
          if exiterr, ok := err.(*exec.ExitError); ok {
            // The program has exited with an exit code != 0

            // This works on both Unix and Windows. Although package
            // syscall is generally platform dependent, WaitStatus is
            // defined for both Unix and Windows and in both cases has
            // an ExitStatus() method with the same signature.
            if status, ok := exiterr.Sys().(syscall.WaitStatus); ok {
              // logger.Info("Exit Status: %d", status.ExitStatus())
              log.Printf("Exit Status: %d",status.ExitStatus())
            }
          } else {
            log.Printf("Error: %v",err)
            // logger.Error("cmd.Wait: %v", err)
            error = err.Error()
          }
        }

        outcome := out.String()
        if outcome == "" {
          outcome = "(empty)"
        }

        // attach the output of the command to the result message
        log.Printf("Command Output: %s", outcome)

        outst := Outcome{Task:task.Code,ErrorCode:error,WorkerCode:worker.Metadata.Code,Outcome:outcome,Pcb:task.Payload.PcbPath}

        body, err := json.Marshal(outst)
        if err != nil {
            log.Printf("Output: %v", err)
        }

        // Send Outcome Message back to queue
        err = ch.Publish(
          "",     // exchange
          outq.Name, // routing key
          false,  // mandatory
          false,  // immediate
          amqp.Publishing{
            ContentType: "application/json",
            Body:        body,
          })
        log.Printf(" [x] Sent %s", body)
        failOnError(err, "Failed to publish a message")

    }
  }()

  log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
  <-forever
}

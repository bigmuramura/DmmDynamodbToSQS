package main

/*
参考
https://qiita.com/ezaki/items/2a9f10c53d958070ca95
https://qiita.com/sakayuka/items/4af7fead94d589716f4d
*/

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/sqs"
)

// DmmDate ... DynamoDBのテーブル形式
type DmmDate struct {
	Date             string `json:"check_date"`
	RemainigCapacity int    `json:"remainig_capacity"`
}

// REGION AWSのデフォルトリージョン指定
const REGION = "ap-northeast-1"

func main() {
	const DBTABLE = "dmm-giga-db"

	sess := session.Must(session.NewSession())
	svc := dynamodb.New(
		sess,
		aws.NewConfig().WithRegion(REGION),
	)

	// 検索キーの日付
	keyDay := time.Now()

	params := &dynamodb.GetItemInput{
		TableName: aws.String(DBTABLE),

		Key: map[string]*dynamodb.AttributeValue{
			"check_date": {
				S: aws.String(keyDay.Format("2006-01-02")), // 日付は文字列で渡す必要が有り、日付自体はDBにこのフォーマットで登録されている
			},
		},
		/*
			AttributesToGet: []*string{
				aws.String("remainig_capacity"),
			},
		*/
	}
	resp, err := svc.GetItem(params)
	if err != nil {
		fmt.Println(err.Error())
	}

	// respの結果を確認
	if len(resp.Item) != 0 {
		/*
			resultCapa := *resp.Item["remainig_capacity"]
			fmt.Println(resultCapa)
		*/

		dmm := &DmmDate{}
		if err := dynamodbattribute.UnmarshalMap(resp.Item, dmm); err != nil {
			fmt.Println("Unmarshal Error", err)
		}

		j, _ := json.Marshal(dmm)
		fmt.Println(string(j))

		// Queueに入れるメッセージの整形（CSV形式）
		msgBody := string(j)
		err = enqueueSqs(msgBody)
		if err != nil {
			fmt.Println("ERROR: ", err)
			os.Exit(1)
		}
	} else {
		// 検索にヒットしなければ終了
		os.Exit(0)
	}
}

func enqueueSqs(msg string) error {
	const (
		QUEUEURL = "https://sqs.ap-northeast-1.amazonaws.com/468866502186/dmm-giga-sqs"
	)

	sess := session.Must(session.NewSession())
	svc := sqs.New(
		sess,
		aws.NewConfig().WithRegion(REGION),
	)

	parms := &sqs.SendMessageInput{
		QueueUrl:     aws.String(QUEUEURL),
		DelaySeconds: aws.Int64(1),
		MessageBody:  aws.String(msg),
	}

	sqsRes, err := svc.SendMessage(parms)
	if err != nil {
		return err
	}

	fmt.Println("SQSMessageID", *sqsRes.MessageId)

	return nil
}

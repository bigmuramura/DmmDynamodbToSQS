package main

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
	"github.com/aws/aws-sdk-go/service/ssm"
)

// DmmData ... DynamoDBのテーブル形式
type DmmData struct {
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
	keyDay := time.Now().Format("2006-01-02")

	params := &dynamodb.GetItemInput{
		TableName: aws.String(DBTABLE),

		Key: map[string]*dynamodb.AttributeValue{
			"check_date": {
				S: aws.String(keyDay), // 日付は文字列で渡す必要が有り、日付自体はDBにこのフォーマットで登録されている
			},
		},
	}
	resp, err := svc.GetItem(params)
	if err != nil {
		fmt.Println(err.Error())
	}

	// respの結果を確認
	if len(resp.Item) != 0 {
		dmm := &DmmData{}
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
		fmt.Printf("%v 対象の日付はDynamoDBに登録なし", keyDay)
		os.Exit(0)
	}
}

func enqueueSqs(msg string) error {
	sess := session.Must(session.NewSession())
	svc := sqs.New(
		sess,
		aws.NewConfig().WithRegion(REGION),
	)

	// SQSのURLをパラメータストアから取得
	parameterName := "DMM-SqsURL"
	res, err := fetchParameterStore(parameterName)
	if err != nil {
		return err
	}
	sqsURL := res

	parms := &sqs.SendMessageInput{
		QueueUrl:     aws.String(sqsURL),
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

func fetchParameterStore(param string) (string, error) {
	sess := session.Must(session.NewSession())
	svc := ssm.New(
		sess,
		aws.NewConfig().WithRegion(REGION),
	)

	res, err := svc.GetParameter(&ssm.GetParameterInput{
		Name:           aws.String(param),
		WithDecryption: aws.Bool(true),
	})
	if err != nil {
		return "Fetch Error", err
	}

	value := *res.Parameter.Value
	return value, nil
}

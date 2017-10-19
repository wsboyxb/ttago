package main

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/endpoints"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/expression"
	"os"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"time"
	"github.com/wsboyxb/tta/conf"
)

const (
	SETP = 10000
)

func main() {
	sess := session.Must(session.NewSession(&aws.Config{
		Region:      aws.String(endpoints.CnNorth1RegionID),
		Credentials: credentials.NewStaticCredentials(conf.AccessKeyID, conf.SecretAccessKey, ""),
	}))

	svc := dynamodb.New(sess)

	//////////////////////////////////////////
	//636238944000000000 2017 03 01
	filt := expression.Name("realActivityTimestamp").GreaterThan(expression.Value(636238944000000000)).
		And(expression.Name("CountryID").AttributeExists())
	//And(expression.Name("level").GreaterThanEqual(expression.Value(10)))

	expr, err := expression.NewBuilder().WithFilter(filt).Build()
	if err != nil {
		fmt.Println("Got error building expression:")
		fmt.Println(err.Error())
		os.Exit(1)
	}

	allItems := make([]map[string]*dynamodb.AttributeValue, 0, 10)
	params := &dynamodb.ScanInput{
		//ExclusiveStartKey:expr.
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		FilterExpression:          expr.Filter(),
		Limit:                     aws.Int64(SETP),
		TableName:                 aws.String("user"),
	}

	result, err := svc.Scan(params)
	if err != nil {
		fmt.Println("Query API call failed:")
		fmt.Println((err.Error()))
		os.Exit(1)
	}

	allItems = append(allItems, result.Items...)
	lastEvaluateKey := result.LastEvaluatedKey
	k := 0
	for ; k < 2 && lastEvaluateKey != nil; {
		k++
		fmt.Println("k = ", k)
		time.Sleep(2 * time.Second)
		result, err = svc.Scan(params)
		if err != nil {
			fmt.Println("Query API call failed:")
			fmt.Println((err.Error()))
			os.Exit(1)
		}
		allItems = append(allItems, result.Items...)

		lastEvaluateKey = result.LastEvaluatedKey
		params = &dynamodb.ScanInput{
			ExclusiveStartKey:         lastEvaluateKey,
			ExpressionAttributeNames:  expr.Names(),
			ExpressionAttributeValues: expr.Values(),
			FilterExpression:          expr.Filter(),
			Limit:                     aws.Int64(SETP),
			TableName:                 aws.String("user"),
		}
	}

	//for _, u := range allItems {
	user := []User{}
	err = dynamodbattribute.UnmarshalListOfMaps(allItems, &user)
	if err != nil {
		fmt.Println("Got error unmarshalling:")
		fmt.Println(err.Error())
		os.Exit(1)
	}

	// Which ones had a higher rating?
	//for _, u := range user {
	//	fmt.Printf("%+v\n", u)
	//}
	//}
	f, err := os.Create("dump.out")
	if err != nil {
		panic(err)
	}
	defer f.Close()
	for _, value := range user {
		fmt.Fprintln(f, value) // print values to f, one per line
	}

	//////////////////////////////////////////

	//result, err := svc.ListTables(&dynamodb.ListTablesInput{})
	//if err != nil {
	//	if aerr, ok := err.(awserr.Error); ok {
	//		switch aerr.Code() {
	//		case dynamodb.ErrCodeInternalServerError:
	//			fmt.Println(dynamodb.ErrCodeInternalServerError, aerr.Error())
	//		default:
	//			fmt.Println(aerr.Error())
	//		}
	//	} else {
	//		// Print the error, cast err to awserr.Error to get the Code and
	//		// Message from an error.
	//		fmt.Println(err.Error())
	//	}
	//	return
	//}
	//
	////fmt.Println(result)
	//for _, n := range result.TableNames {
	//	fmt.Println(*n)
	//}
}

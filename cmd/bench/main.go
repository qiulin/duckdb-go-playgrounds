package main

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"math/rand/v2"
	"net/http"
	"time"

	"github.com/lynx-go/x/encoding/json"
	"github.com/lynx-go/x/log"
)

func main() {

	ctx := context.Background()
	for i := 0; i < 1_000_000; i++ {
		if err := postWrite(ctx); err != nil {
			panic(err)
		}
		//time.Sleep(time.Duration(rand.IntN(10)) * time.Millisecond)
	}
}

func postWrite(ctx context.Context) error {
	userId := randUserId()
	roomId := randRoomId()
	roomType := randRoomType()
	serverId := randServerId()

	payload := map[string]interface{}{
		"user_id":    userId,
		"room_id":    roomId,
		"room_type":  roomType,
		"server_id":  serverId,
		"created_at": time.Now().UnixMilli(),
	}
	jsonPayload, _ := json.Marshal(payload)

	log.InfoContext(ctx, "request", "body", string(jsonPayload))
	resp, err := http.Post("http://127.0.0.1:7070/api/write", "application/json", bytes.NewBuffer(jsonPayload))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("bad status: %s", resp.Status)
	}
	body, _ := ioutil.ReadAll(resp.Body)
	respBody := map[string]interface{}{}
	if err := json.Unmarshal(body, &respBody); err != nil {
		return err
	}
	log.InfoContext(ctx, "response", "body", json.MustMarshalToString(respBody))
	return nil
}

func randRoomId() int {
	return rand.IntN(30) + 1000
}

func randUserId() int {
	return rand.IntN(500) + 1000000
}

func randRoomType() int {
	n := rand.IntN(len(roomTypes))
	return roomTypes[n]
}

func randServerId() int {
	n := rand.IntN(len(serverIds))
	return serverIds[n]
}

var serverIds = []int{1, 2, 3, 3, 4, 5, 6, 7, 8, 9, 10}

var roomTypes = []int{1, 2, 3, 4, 5, 6, 7, 8}

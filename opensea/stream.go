package opensea

import (
	"fmt"
	"github.com/foundVanting/opensea-stream-go/types"
	"github.com/xiaowang7777/phx"
	"net/url"
	"sync"
)

type StreamClient struct {
	socket   *phx.Socket
	channels *sync.Map
}

func NewStreamClient(network types.Network, token string, logger phx.Logger, onError func(error)) *StreamClient {
	m := map[types.Network]string{
		types.MAINNET: "wss://stream.openseabeta.com/socket",
		types.TESTNET: "wss://testnets-stream.openseabeta.com/socket",
	}
	socketUrl := fmt.Sprintf("%s?token=%s", m[network], token)

	endPoint, _ := url.Parse(socketUrl)
	socket := phx.NewSocket(endPoint)

	socket.OnError(onError)
	socket.OnClose(
		func() {
			err := socket.Reconnect()
			if err != nil {
				onError(err)
			}
		},
	)
	socket.Logger = logger
	return &StreamClient{
		socket:   socket,
		channels: &sync.Map{},
	}
}

func (s *StreamClient) Connect() error {
	return s.socket.Connect()
}

func (s *StreamClient) Disconnect() error {
	s.socket.Logger.Println(phx.LogInfo, "socket", "succesfully disconnected from socket")
	s.channels = &sync.Map{}
	return s.socket.Disconnect()
}

func (s *StreamClient) createChannel(topic string) (*phx.Channel, error) {
	channel := s.socket.Channel(topic, nil)
	_, err := channel.Join()
	if err != nil {
		return nil, err
	}
	s.channels.Store(topic, channel)
	return channel, nil
}

func (s *StreamClient) getChannel(topic string) (*phx.Channel, error) {
	value, ok := s.channels.Load(topic)
	if ok {
		return value.(*phx.Channel), nil
	}

	return s.createChannel(topic)
}

func (s *StreamClient) On(eventsType []types.EventType, collectionSlug string, callback func(payload any)) (func() error, error) {
	topic := collectionTopic(collectionSlug)
	channel, err := s.getChannel(topic)
	if err != nil {
		return nil, err
	}

	for _, eventType := range eventsType {
		s.socket.Logger.Printf(phx.LogInfo, "channel", "subscribing to %s events on %s\n", eventType, topic)
		channel.On(string(eventType), callback)
	}

	return func() error {
		s.socket.Logger.Println(phx.LogInfo, "channel", "unsubscribing from all events on %s", topic)
		leave, err := channel.Leave()
		if err != nil {
			return err
		}

		leave.Receive(
			"ok",
			func(response any) {
				s.channels.Delete(topic)
			},
		)
		return nil
	}, nil
}

func (s *StreamClient) OnItemListed(collectionSlug string, Callback func(itemListedEvent any)) (func() error, error) {
	return s.On([]types.EventType{types.ItemListed}, collectionSlug, Callback)
}

func (s *StreamClient) OnItemSold(collectionSlug string, Callback func(itemSoldEvent any)) (func() error, error) {
	return s.On([]types.EventType{types.ItemSold}, collectionSlug, Callback)
}

func (s *StreamClient) OnItemTransferred(collectionSlug string, Callback func(itemTransferredEvent any)) (func() error, error) {
	return s.On([]types.EventType{types.ItemTransferred}, collectionSlug, Callback)
}

func (s *StreamClient) OnItemCancelled(collectionSlug string, Callback func(itemCancelledEvent any)) (func() error, error) {
	return s.On([]types.EventType{types.ItemCancelled}, collectionSlug, Callback)
}

func (s *StreamClient) OnItemReceivedBid(collectionSlug string, Callback func(itemReceivedBidEvent any)) (func() error, error) {
	return s.On([]types.EventType{types.ItemReceivedBid}, collectionSlug, Callback)
}

func (s *StreamClient) OnItemReceivedOffer(collectionSlug string, Callback func(itemReceivedOfferEvent any)) (func() error, error) {
	return s.On([]types.EventType{types.ItemReceivedOffer}, collectionSlug, Callback)
}

func (s *StreamClient) OnItemMetadataUpdated(collectionSlug string, Callback func(itemMetadataUpdatedEvent any)) (func() error, error) {
	return s.On([]types.EventType{types.ItemMetadataUpdated}, collectionSlug, Callback)
}

func collectionTopic(slug string) string {
	return fmt.Sprintf("collection:%s", slug)
}

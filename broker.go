//шина событий
//реализация функционала брокера сообщений

package eventbus

import (
	"encoding/json"
	"net"
	"reflect"
	"strconv"
	"sync"
	"time"

	gracefultcpserver "github.com/NemanCat/gracefultcpserver"
	memoryhashmap "github.com/NemanCat/memoryhashmap"
	"github.com/google/uuid"
)

// -----------------------------------------------------------
// тип Консьюмер
// используется для хранения данных подключённых к шине консьюмеров
type Consumer struct {
	//адрес консьюмера
	Addr string
	//список кодов сообщений на которые подписан консьюмер
	Topics []string
}

// декодирование типа Консьюмер записанного в БД Bolt
func (consumer *Consumer) UnmarshalBaseObjectJSON(encoded []byte) string {
	err := json.Unmarshal(encoded, consumer)
	if err != nil {
		return err.Error()
	}
	return ""
}

// тип Получатель сообщения о событии
// элемент списка получателей в записи сообщения в БД Bolt
type EventReceiver struct {
	//адрес получателя
	Addr string
	//отправлено ли сообщение
	Sent bool
}

// декодирование типа Получатель сообщения записанного в БД Bolt
func (receiver *EventReceiver) UnmarshalBaseObjectJSON(encoded []byte) string {
	err := json.Unmarshal(encoded, receiver)
	if err != nil {
		return err.Error()
	}
	return ""
}

// тип Сообщение о событии
type BrokerEventMessage struct {
	//дата и время получения сообщения в формате unix timestamp
	Received int64
	//полученное сообщение
	Data EventBusMessage
	//список получателей сообщения
	Receivers map[string]EventReceiver
}

// декодирование типа Консьюмер записанного в БД Bolt
func (em *BrokerEventMessage) UnmarshalBaseObjectJSON(encoded []byte) string {
	err := json.Unmarshal(encoded, em)
	if err != nil {
		return err.Error()
	}
	return ""
}

// ----------------------------------------------------------
// тип Брокер шины событий
type EventBroker struct {
	//безопасный tcp сервер
	gs *gracefultcpserver.GracefulTCPServer
	//список зарегистрировавшихся консьюмеров
	consumers *memoryhashmap.PersistentMemoryHashMap
	//список сообщений о событиях
	event_messages *memoryhashmap.PersistentMemoryHashMap
	//пользовательский список числовых кодов сообщений
	custom_event_message_codes map[uint8]string
	//пользовательский обработчик ошибок
	custom_error_handler func(error)
	//пользовательская функция записи сообщения в лог
	custom_write_log func(string)
	//интервал запуска сервиса отправки неотправленных сообщений, секунд
	send_interval int
	//срок хранения отправленных всем адресатам сообщений, часов
	delivered_storage_time int
	//срок хранения не отправленных всем адресатам сообщений, часов
	undelivered_storage_time int
	//wait group для остановки дочернего сервиса отправки неотправленных сообщений
	wg sync.WaitGroup
	//канал сигнала остановки сервера
	quit_channel chan interface{}
}

// ----------------------------------------------------------------
// создание и запуск экземпляра брокера шины событий
func RunEventBroker(port string,
	dbfolder string,
	custom_event_message_codes map[uint8]string,
	custom_error_handler func(error),
	custom_write_log func(string),
	send_interval int,
	delivered_storage_time int,
	undelivered_storage_time int) (*EventBroker, error) {
	var err error
	evb := new(EventBroker)
	//создаем список консумеров
	evb.consumers, err = memoryhashmap.CreatePersistentMemoryHashMap(reflect.TypeOf(Consumer{}), dbfolder, "broker_consumers.db")
	if err != nil {
		return nil, err
	}
	//создаем список сообщений о событиях
	evb.event_messages, err = memoryhashmap.CreatePersistentMemoryHashMap(reflect.TypeOf(BrokerEventMessage{}), dbfolder, "broker_event_messages.db")
	if err != nil {
		return nil, err
	}
	//custom список кодов сообщений
	evb.custom_event_message_codes = custom_event_message_codes
	//custom обработчик ошибок
	evb.custom_error_handler = custom_error_handler
	//пользовательская функция записи в лог
	evb.custom_write_log = custom_write_log
	//интервал запуска сервиса отправки неотправленных сообщений
	evb.send_interval = send_interval
	//срок хранения отправленных всем адресатам сообщений, часов
	evb.delivered_storage_time = delivered_storage_time
	//срок хранения не отправленных всем адресатам сообщений, часов
	evb.undelivered_storage_time = undelivered_storage_time
	//создаём и запускаем безопасный tcp сервер
	evb.gs, err = gracefultcpserver.RunGracefulTCPServer(":"+port, evb.handleConnection)
	if err != nil {
		return nil, err
	}
	//создаём канал сигнала остановки сервера
	evb.quit_channel = make(chan interface{})
	//запускаем дочерний сервис отправки неотправленных сообщений и добавляем его в wait group
	evb.wg.Add(1)
	go func() {
		ticker := time.NewTicker(time.Second * time.Duration(evb.send_interval))
		defer ticker.Stop()
		for {
			select {
			case <-evb.quit_channel:
				//получен сигнал остановки
				evb.wg.Done()
				return
			case <-ticker.C:
				//запускаем процедуру отправки неотправленных сообщений
				evb.sendUndeliveredMessages()
			default:
				//ничего не делаем
			}
		}
	}()
	return evb, nil
}

// безопасная остановка экземпляра брокера шины событий
func (evb *EventBroker) Stop() {
	close(evb.quit_channel)
	evb.wg.Wait()
	evb.gs.Stop()
}

// обработчик подключений брокера сообщений
// conn tcp connection
func (evb *EventBroker) handleConnection(conn net.Conn) {
	defer conn.Close()
	// получаем сообщение
	message, err := ReceiveEventMessage(conn)
	if err != nil {
		//передаём ошибку пользовательскому обработчику ошибок
		evb.custom_error_handler(err)
		return
	}
	if message.Type < 10 {
		//это системное сообщение - числовые коды пользовательских сообщений начинаются с 10
		//декодируем сообщение
		t := System_event_message_types[message.Type]
		el := reflect.New(t.Elem())
		el_pointer := el.Interface()
		err = json.Unmarshal(message.Body, el_pointer)
		if err != nil {
			//передаём ошибку пользовательскому обработчику ошибок
			evb.custom_error_handler(err)
			return
		}
		//вызываем обработчик системных сообщений
		evb.handleSystemConnection(message.Type, reflect.Indirect(el))
	} else {
		//это пользовательское сообщение - вызываем обработчик пользовательских сообщений
		evb.handleCustomConnection(message.Type, *message)
	}
}

// обработчик системных сообщений
func (evb *EventBroker) handleSystemConnection(message_type uint8, value reflect.Value) {
	switch message_type {
	case uint8(0):
		//регистрация консьюмера на брокере
		//данные консьюмера
		name := value.FieldByName("Name").Interface().(string)
		addr := value.FieldByName("Addr").Interface().(string)
		topics := value.FieldByName("Topics").Interface().([]string)
		//добавляем консьюмера в список зарегистрировавшихся консьюмеров
		new_consumer := Consumer{Addr: addr, Topics: topics}
		evb.consumers.AddUpdateObject(name, &new_consumer)
		//проверяем имеются ли неотправленные сообщения для вновь подключившегося консьюмера
		for key, event_message := range evb.event_messages.GetList() {
			topic := evb.custom_event_message_codes[event_message.(*BrokerEventMessage).Data.Type]
			for _, consumer_topic := range topics {
				if topic == consumer_topic {
					//проверяем имеется ли данный консьюмер в списке получателей сообщения
					receiver, ok := event_message.(*BrokerEventMessage).Receivers[name]
					tmp_message := BrokerEventMessage{
						Received:  event_message.(*BrokerEventMessage).Received,
						Data:      event_message.(*BrokerEventMessage).Data,
						Receivers: event_message.(*BrokerEventMessage).Receivers,
					}
					if ok {
						if !receiver.Sent {
							tmp_receiver := receiver
							//пытаемся отправить консьюмеру сообщение
							err := event_message.(*BrokerEventMessage).Data.Send(addr)
							if err != nil {
								tmp_receiver.Sent = false
							} else {
								tmp_receiver.Sent = true
							}
							tmp_message.Receivers[name] = tmp_receiver
						}
					} else {
						//пытаемся отправить консьюмеру сообщение и заносим его в список получателей
						success := true
						err := event_message.(*BrokerEventMessage).Data.Send(addr)
						if err != nil {
							success = false
						}
						tmp_message.Receivers[name] = EventReceiver{Addr: addr, Sent: success}
					}
					evb.event_messages.AddUpdateObject(key, &tmp_message)
				}
			}
		}
	case uint8(1):
		//отключение консьюмера от брокера
		name := value.FieldByName("Name").Interface().(string)
		evb.consumers.DeleteObject(name)
	default:
		//неопределённое сообщение - ничего не делаем
	}

}

// обработчик пользовательских сообщений
func (evb *EventBroker) handleCustomConnection(message_type uint8, message EventBusMessage) {
	var consumer *Consumer
	sent := false
	receivers := make(map[string]EventReceiver)
	//составляем список получателей сообщения и пытаемся отправить им сообщение
	for key, value := range evb.consumers.GetList() {
		consumer = value.(*Consumer)
		for _, topic := range consumer.Topics {
			if topic == evb.custom_event_message_codes[message_type] {
				err := message.Send(consumer.Addr)
				if err != nil {
					sent = false
				} else {
					sent = true
				}
			}
		}
		receivers[key] = EventReceiver{Addr: consumer.Addr, Sent: sent}
	}

	//генерируем уникальный ID сообщения
	uuid := uuid.NewString()
	//дата и время получения события
	received := time.Now().Unix()
	//сохраняем сообщение в списке сообщений
	br_message := BrokerEventMessage{Received: received, Data: message, Receivers: receivers}
	evb.event_messages.AddUpdateObject(uuid, &br_message)
}

// функционал отправки неотправленных сообщений и удаления устаревших записей
func (evb *EventBroker) sendUndeliveredMessages() {
	var delivered bool
	//количество удалённых старых сообщений доставленных всем получателям
	deleted_delivered_messages := 0
	//количество удалённых старых сообщений не доставленных всем получателям
	deleted_undelivered_messages := 0
	//обходим список сообщений
	for key, value := range evb.event_messages.GetList() {
		delivered = true
		for _, receiver := range value.(*BrokerEventMessage).Receivers {
			if !receiver.Sent {
				//это сообщение не было доставлено всем получателям
				delivered = false
				break
			}
		}
		//проверяем устарело ли сообщение
		to_remove := false
		//дата и время добавления сообщения в шину событий
		received_time := time.Unix(value.(*BrokerEventMessage).Received, 3)
		//текущие дата и время
		current_time := time.Now()
		//разница между ними
		diff := current_time.Sub(received_time)
		//надо ли удалять сообщение
		if delivered {
			to_remove = diff.Hours() >= float64(evb.delivered_storage_time)
		} else {
			to_remove = diff.Hours() >= float64(evb.undelivered_storage_time)
		}
		if to_remove {
			//удаляем сообщение из списка сообщений
			evb.event_messages.DeleteObject(key)
			if delivered {
				deleted_delivered_messages++
			} else {
				deleted_undelivered_messages++
			}
		} else {
			if !delivered {
				//имеются получатели которым не доставлено данное сообщение
				tmp_message := BrokerEventMessage{
					Received:  value.(*BrokerEventMessage).Received,
					Data:      value.(*BrokerEventMessage).Data,
					Receivers: value.(*BrokerEventMessage).Receivers,
				}
				for name, receiver := range value.(*BrokerEventMessage).Receivers {
					if !receiver.Sent {
						//пытаемся отправить сообщение повторно
						err := value.(*BrokerEventMessage).Data.Send(receiver.Addr)
						if err == nil {
							receiver.Sent = true
						}
						tmp_message.Receivers[name] = receiver
					}
				}
				evb.event_messages.AddUpdateObject(key, &tmp_message)
			}
		}
	}
	//если были удалены старые сообщения - делаем запись в лог
	if (deleted_delivered_messages > 0) || (deleted_undelivered_messages > 0) {
		evb.custom_write_log("Удалено старых сообщений: " + strconv.Itoa(deleted_delivered_messages+deleted_undelivered_messages))
	}
}

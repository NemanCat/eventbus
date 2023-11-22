//шина событий
//реализация функционала консьюмера шины событий

package eventbus

import (
	"encoding/json"
	"net"
	"reflect"

	gracefultcpserver "github.com/NemanCat/gracefultcpserver"
)

// -----------------------------------------------------
// тип Консьюмер шины событий
type EventConsumer struct {
	//имя консьюмера
	name string
	//адрес консьюмера в формате <хост>:<порт>
	addr string
	//символьные коды типов сообщений на которые подписан консьюмер
	topics []string
	//адрес брокера шины событий
	broker_addr string
	//безопасный tcp сервер консьюмера
	gs *gracefultcpserver.GracefulTCPServer
	//custom список типов сообщений
	custom_event_message_types map[uint8]reflect.Type
	//custom обработчик входящих сообщений
	custom_handler func(uint8, reflect.Value)
	//custom обработчик ошибок
	custom_error_handler func(error)
}

// создание и запуск экземпляра консьюмера шины событий
// name имя консьюмера
// addr адрес консьюмера
// topics список символьных кодов типов сообщений на которые подписан консьюмер
// broker_addr адрес брокера сообщений шины событий
// custom_event_message_types справочник пользовательских типов сообщений
// custom_handler пользовательский обработчик событий
// custom_error_handler пользовательский обработчик ошибок
func RunEventConsumer(name string,
	addr string,
	topics []string,
	broker_addr string,
	custom_event_message_types map[uint8]reflect.Type,
	custom_handler func(uint8, reflect.Value),
	custom_error_handler func(error)) (*EventConsumer, error) {
	var err error
	evc := new(EventConsumer)
	evc.name = name
	evc.addr = addr
	evc.topics = topics
	evc.broker_addr = broker_addr
	evc.custom_event_message_types = custom_event_message_types
	evc.custom_handler = custom_handler
	evc.custom_error_handler = custom_error_handler
	//создаём и запускаем безопасный tcp сервер
	evc.gs, err = gracefultcpserver.RunGracefulTCPServer(addr, evc.handleConnection)
	if err != nil {
		return nil, err
	}
	return evc, nil
}

// безопасная остановка экземпляра консьюмера
func (evc *EventConsumer) Stop() {
	evc.gs.Stop()
}

// системный обработчик входящих сообщений консьюмера
// получает из tcp connection и декодирует сообщение после чего передаёт его
// для дальнейше обработки в custom handler
// conn tcp connection
func (evc *EventConsumer) handleConnection(conn net.Conn) {
	defer conn.Close()
	// парсим полученное сообщение
	// получаем сообщение
	message, err := ReceiveEventMessage(conn)
	if err != nil {
		evc.custom_error_handler(err)
		return
	}
	//декодируем сообщение
	t := evc.custom_event_message_types[message.Type]
	el := reflect.New(t.Elem())
	el_pointer := el.Interface()
	err = json.Unmarshal(message.Body, el_pointer)
	if err != nil {
		evc.custom_error_handler(err)
		return
	}
	//вызываем custom handler для дальнейшей обработки сообщения
	evc.custom_handler(message.Type, reflect.Indirect(el))
}

// регистрация/дерегистрация консюмера на брокере шины событий
// register = true регистрация на брокере
// register = false дерегистрация на брокере
// my_addr адрес консьюмера
func (evc *EventConsumer) RegisterUnregisterEventConsumer(register bool, my_addr string) error {
	var opcode uint8
	if register {
		//регистрация
		opcode = uint8(0)
	} else {
		//дерегистрация
		opcode = uint8(1)
	}

	registration_message, err := CreateEventBusMessage(opcode, ConsumerRegistrationMessage{Name: evc.name, Addr: my_addr, Topics: evc.topics})
	if err != nil {
		return err
	}
	err = registration_message.Send(evc.broker_addr)
	if err != nil {
		return err
	}
	return nil
}

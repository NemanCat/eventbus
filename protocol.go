//шина событий
//реализация tcp - протокола обмена данными между объектами шины событий

package eventbus

import (
	"encoding/binary"
	"encoding/json"
	"net"
)

// ----------------------------------------------------
// структура tcp - сообщения
type EventBusMessage struct {
	//код типа сообщения
	//коды от 0 до 9 включительно зарезервированы для системных сообщений
	//коды больше 10 могут использоваться для пользовательских типов сообщений
	Type uint8
	//длина сообщения
	Length uint32
	//тело сообщения
	Body []byte
}

// функция декодирования tcp - сообщения
// требуется для сохранения сообщения в БД Bolt
// декодирование типа EventMessage записанного в БД Bolt
func (em *EventBusMessage) UnmarshalBaseObjectJSON(encoded []byte) string {
	err := json.Unmarshal(encoded, em)
	if err != nil {
		return err.Error()
	}
	return ""
}

// создание сообщения
// typ uint8           числовой код сообщения
// message interface{} тело сообщения
func CreateEventBusMessage(typ uint8, message interface{}) (*EventBusMessage, error) {
	//кодируем в json тело сообщения
	body, err := json.Marshal(message)
	if err != nil {
		return nil, err
	}
	em := new(EventBusMessage)
	em.Type = typ
	em.Length = uint32(len(body))
	em.Body = body
	return em, nil
}

// получение сообщения из tcp - соединения
// conn - tcp connection
func ReceiveEventMessage(conn net.Conn) (*EventBusMessage, error) {
	var type_code uint8
	var length uint32
	defer conn.Close()
	//считываем числовой код типа сообщения
	err := binary.Read(conn, binary.LittleEndian, &type_code)
	if err != nil {
		return nil, err
	}
	//считываем длину сообщения
	err = binary.Read(conn, binary.LittleEndian, &length)
	if err != nil {
		return nil, err
	}
	//считываем тело сообщения
	body := make([]byte, length)
	_, err = conn.Read(body)
	if err != nil {
		return nil, err
	}
	//формируем и возвращаем сообщение
	em := new(EventBusMessage)
	em.Type = type_code
	em.Length = uint32(len(body))
	em.Body = body
	return em, nil
}

// отправка сообщения по указанному адресу
// addr адрес объекта - получателя сообщения в формате <хост>:<порт>
func (em *EventBusMessage) Send(addr string) error {
	server, err := net.ResolveTCPAddr("tcp", addr)
	if err != nil {
		return err
	}
	//подключаемся к серверу
	conn, err := net.DialTCP("tcp", nil, server)
	if err != nil {
		return err
	}
	defer conn.Close()
	//отправляем на сервер тип сообщения
	err = binary.Write(conn, binary.LittleEndian, em.Type)
	if err != nil {
		return err
	}
	//отправляем на сервер длину сообщения
	err = binary.Write(conn, binary.LittleEndian, em.Length)
	if err != nil {
		return err
	}
	//отправляем на сервер тело сообщения
	_, err = conn.Write([]byte(em.Body))
	if err != nil {
		return err
	}
	conn.Close()
	return nil
}

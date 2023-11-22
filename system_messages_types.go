//шина событий
//справочник системных сообщений

package eventbus

import (
	"reflect"
)

// ------------------------------------------------------------------
var System_event_message_types = map[uint8]reflect.Type{
	//регистрация консьюмера на брокере
	uint8(0): reflect.TypeOf(&ConsumerRegistrationMessage{}),
	//отключение консьюмера от брокера
	uint8(1): reflect.TypeOf(&ConsumerRegistrationMessage{}),
}

// ------------------------------------------------------------------
// структура запроса консьюмера о регистрации в списке консьюмеров
type ConsumerRegistrationMessage struct {
	//имя консьюмера
	Name string
	//адрес консьюмера
	Addr string
	//список кодов сообщений на которые подписан консьюмер
	Topics []string
}

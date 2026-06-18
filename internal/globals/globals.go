package globals

const MAX_PACKET_SIZE int = 4096

// Status Codes
const OK_STATUS_CODE uint8 = 0

const PUBLISHER_TYPE uint8 = 0
const SUBSCRIBER_TYPE uint8 = 1
const SERVICE_TYPE uint8 = 2
const SERVICE_CALLER_TYPE uint8 = 3

const ERROR_SERIALIZER_ERROR_CODE uint8 = 251
const ERROR_SERVICE_ERROR_CODE uint8 = 252
const ERROR_INVALID_OPERATION_CODE uint8 = 253
const ERROR_MISMATCH_PAYLOAD_CODE uint8 = 254
const ERROR_HANDLER_INTERNAL_ERROR_CODE uint8 = 255

// Spined Codes
const SPINED_REGISTER uint8 = 0
const SPINED_GET_INFO uint8 = 1

const ERROR_SERVICE_HANDLER = "service handler has an error"
const ERROR_CORRUPT_PAYLOAD = "CORRUPT_PAYLOAD"
const ERROR_PAYLOAD_SIZE = "failed to encode key. key is too big. max key size is 4kb"

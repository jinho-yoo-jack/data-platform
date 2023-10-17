package kcp.data.common.response;


import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@NoArgsConstructor
@Getter
@Builder
public class ResponseMessage<T> {
    private String status; // 2xx, 3xx, 4xx, 5xx
    private T data;
    private String message;
    private String exceptionName;

    private ResponseMessage(String status, T data, String message, String exception){
        this.status = status;
        this.data = data;
        this.message = message;
        this.exceptionName = exception;
    }

    public static <T> ResponseMessage<T> createSuccessRespMessage(T data){
        return ResponseMessage.<T>builder()
            .status("200")
            .data(data)
            .message(null)
            .exceptionName(null)
            .build();
    }

    public static ResponseMessage<?> createErrorRespMessage(String status, String message, String exception){
        return ResponseMessage.builder()
            .status(status)
            .data(null)
            .message(message)
            .exceptionName(exception)
            .build();
//        return new ResponseMessage<>(status, null, message, exception);
    }
}

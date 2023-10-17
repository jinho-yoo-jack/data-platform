package kcp.data.common.exception;

import kcp.data.common.response.ResponseMessage;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.io.IOException;

@RestControllerAdvice
public class DataSourceControllerAdvice {

    @ExceptionHandler(value={IOException.class})
    public ResponseMessage<?> ioExceptionHandler(IOException e){
        return ResponseMessage.createErrorRespMessage("500", e.getMessage(), e.getClass().getName());
    }
}

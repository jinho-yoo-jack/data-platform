package kcp.data.hadoop.config;

import kcp.data.hadoop.dto.RespFileStatuses;
import okhttp3.MultipartBody;
import okhttp3.RequestBody;
import retrofit2.Call;
import retrofit2.http.*;

import java.util.HashMap;

public interface WebHdfsService {
    @GET("/webhdfs/v1/{directoryPath}?op=LISTSTATUS")
    public Call<RespFileStatuses> getListDirectory(@Path("directoryPath") String directoryPath);

    /* curl -i -X PUT "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=MKDIRS[&permission=<OCTAL>]" */
    @PUT("/webhdfs/v1/{directoryPath}?op=MKDIRS&user.name=black")
    public Call<HashMap<String, Boolean>> createNewDirectory(@Path("directoryPath") String directoryPath);


    /* curl -i -X PUT "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=CREATE */
    @PUT("/webhdfs/v1/{includedPathFileName}?op=CREATE&user.name=black")
    public Call<Void> createEmptyNewFile(@Path("includedPathFileName") String includedPathFileName);

    @Multipart
    @PUT("/webhdfs/v1/{includedPathFileName}?op=CREATE&user.name=black")
    public Call<Void> insertDataToFile(@Path("includedPathFileName") String includedPathFileName,
                                       @Part RequestBody file);
//    @Part MultipartBody.Part file);

    /* curl -i -X POST "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=APPEND[&buffersize=<INT>]" */
    @PUT("/webhdfs/v1/{includedPathFileName}?op=APPEND&user.name=black")
    public Call<Void> appendToFile(@Path("includedPathFileName") String includedPathFileName);


}

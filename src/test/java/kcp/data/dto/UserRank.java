package kcp.data.dto;

import lombok.Data;

import java.io.Serializable;

@Data
public class UserRank implements Serializable {
    private int rank;
    private int userId;

    public UserRank(int userId, int rank){
        this.userId = userId;
        this.rank = rank;
    }
}

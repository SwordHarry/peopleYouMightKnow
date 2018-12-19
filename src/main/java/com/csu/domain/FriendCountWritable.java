package com.csu.domain;

import org.apache.hadoop.io.Writable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

// 自定义 Writable 对象
public class FriendCountWritable implements Writable {
    private Long user;
    private Long mutualFriend;

    public FriendCountWritable(Long user,Long mutualFriend) {
        this.user = user;
        this.mutualFriend = mutualFriend;
    }

    public FriendCountWritable () {
        this(-1L,-1L);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(user);
        dataOutput.writeLong(mutualFriend);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        user = dataInput.readLong();
        mutualFriend = dataInput.readLong();
    }

    @Override
    public String toString (){
        return " toUser:"
                + Long.toString(user) + " mutualFriend: "
                + Long.toString(mutualFriend);
    }

    public Long getUser() {
        return user;
    }

    public void setUser(Long user) {
        this.user = user;
    }

    public Long getMutualFriend() {
        return mutualFriend;
    }

    public void setMutualFriend(Long mutualFriend) {
        this.mutualFriend = mutualFriend;
    }
}

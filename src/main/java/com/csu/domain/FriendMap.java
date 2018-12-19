package com.csu.domain;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.StringTokenizer;

public class FriendMap extends Mapper<LongWritable, Text, LongWritable, FriendCountWritable> {
    private Text word = new Text();

    // map 函数 会自动将文件分成 偏移量(key)， 文本内容(value) 的形式
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line[] = value.toString().split("\\t");
        Long fromUser = Long.parseLong(line[0]);
        List toUsers = new LinkedList<Long>();

        // 如果该用户有好友
        if (line.length == 2) {
            StringTokenizer tokenizer = new StringTokenizer(line[1], ",");
            while (tokenizer.hasMoreTokens()) {
                Long toUser = Long.parseLong(tokenizer.nextToken());
                toUsers.add(toUser);
                // 写入 context中， fromUser 和 这些 toUser 是好友关系了
                // context 中的内容为 三元组 <fromUser,toUser,-1> 意思为fromUser和toUser是共同好友了
                context.write(new LongWritable(fromUser), new FriendCountWritable(toUser, -1L));
            }

            for (int i = 0; i < toUsers.size(); i++) {
                for (int j = i + 1; j < toUsers.size(); j++) {
                    // 给 toUsers[i] 推荐 toUsers[j] 因为他们有 共同好友 fromUser
                    context.write(new LongWritable((Long) toUsers.get(i)),
                            new FriendCountWritable((Long) toUsers.get(j), fromUser));
                    // 给 toUsers[j] 推荐 toUsers[i] 因为他们有 共同好友 fromUser
                    context.write(new LongWritable((Long) toUsers.get(j)),
                            new FriendCountWritable((Long)toUsers.get(i), fromUser));
                }
            }
        }
    }

}

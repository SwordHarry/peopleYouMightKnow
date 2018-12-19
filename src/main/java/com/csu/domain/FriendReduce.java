package com.csu.domain;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

public class FriendReduce extends Reducer<LongWritable, FriendCountWritable, LongWritable, Text> {

    @Override
    public void reduce(LongWritable  key, Iterable<FriendCountWritable> values, Context context) throws IOException, InterruptedException  {
        // key 是当前的主角好友, 而 values 是一个 共同好友的列表

        // 建立一个 共同好友列表的 映射关系
        /**
         *  <userId, [id1, id2, id3, ...]>
         *
         * */
        final java.util.Map<Long, List> mutualFriends = new HashMap<>();

        // 遍历 values , values 的格式 经过 shuffle 之后变成了 可迭代的具有相同 userId 但是有不同 共同好友id 的容器
        for (FriendCountWritable val: values) {
            final boolean isAlreadyFriend = (val.getMutualFriend() == -1);
            final Long toUser = val.getUser();
            final Long mutualFriend = val.getMutualFriend();

            if (mutualFriends.containsKey(toUser)) {
                if (isAlreadyFriend) {
                    mutualFriends.put(toUser, null);
                } else if (mutualFriends.get(toUser) != null) {
                    mutualFriends.get(toUser).add(mutualFriend);
                }
            } else {
                if (!isAlreadyFriend) {
                    mutualFriends.put(toUser,new LinkedList(){
                        {
                            add(mutualFriend);
                        }
                    });
                } else {
                    mutualFriends.put(toUser, null);
                }
            }
        }

        java.util.SortedMap<Long, List<Long>> sortedMutualFriends = new TreeMap<>(new Comparator<Long>() {
            @Override
            public int compare(Long key1, Long key2) {
                Integer v1 = mutualFriends.get(key1).size();
                Integer v2 = mutualFriends.get(key2).size();
                if (v1 > v2) {
                    return -1;
                } else if (v1.equals(v2) && key1 < key2) {
                    return -1;
                } else {
                    return 1;
                }
            }
        });


        for (java.util.Map.Entry<Long, List> entry : mutualFriends.entrySet()) {
            if (entry.getValue() != null) {
                sortedMutualFriends.put(entry.getKey(), entry.getValue());
            }
        }

        // 写入到 output 中，其内容如下：
        /*
        * 推荐的好友id (共同好友个数: 共同的好友列表)
        * */
        int i = 0;
        StringBuilder output = new StringBuilder();
        for (java.util.Map.Entry<Long, List<Long>> entry : sortedMutualFriends.entrySet()) {
            if (i == 0) {
                output = new StringBuilder(entry.getKey().toString() + " (" + entry.getValue().size() + ": " + entry.getValue() + ")");
            } else {
                output.append(",").append(entry.getKey().toString()).append(" (").append(entry.getValue().size()).append(": ").append(entry.getValue()).append(")");
            }
            ++i;
        }
        context.write(key, new Text(output.toString()));
    }
}

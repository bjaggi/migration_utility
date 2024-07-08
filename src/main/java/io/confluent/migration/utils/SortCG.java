package io.confluent.migration.utils;

import io.confluent.model.ConsumerGroupMetdata;

import java.util.*;

public class SortCG {



    public static ArrayList<ConsumerGroupMetdata> sortByCGCommittedOffsetTime(Map<ConsumerGroupMetdata, Long>  cgMetaDataMap){


        List<Map.Entry<ConsumerGroupMetdata,Long>> entries = new ArrayList<Map.Entry<ConsumerGroupMetdata,Long>>(
                cgMetaDataMap.entrySet()
        );

        ArrayList<Map.Entry<ConsumerGroupMetdata, Long>> sorted = new ArrayList<>(cgMetaDataMap.entrySet());
        sorted.sort(Comparator.comparingLong(Map.Entry::getValue));
        ArrayList<ConsumerGroupMetdata> sortedOutput = new ArrayList<>();


        for (Map.Entry<ConsumerGroupMetdata,Long> e : sorted) {
            // This loop prints entries. You can use the same loop
            // to get the keys from entries, and add it to your target list.
            System.out.println("CG Name : "+ e.getKey().getConsumerGroupName()  + " , And Topic Name : "+ e.getKey().getTopicName()+" , has a Timestamp of : "+e.getValue());
            sortedOutput.add(e.getKey());
        }
        return sortedOutput;
    }


    public static ArrayList<ConsumerGroupMetdata> sortByCGLag(Map<ConsumerGroupMetdata, Long>  cgMetaDataMap){
        List<Map.Entry<ConsumerGroupMetdata,Long>> entries = new ArrayList<Map.Entry<ConsumerGroupMetdata,Long>>(
                cgMetaDataMap.entrySet()
        );

        ArrayList<Map.Entry<ConsumerGroupMetdata, Long>> sorted = new ArrayList<>(cgMetaDataMap.entrySet());
        sorted.sort(Collections.reverseOrder(Comparator.comparingLong(Map.Entry::getValue)));
        ArrayList<ConsumerGroupMetdata> sortedOutput = new ArrayList<>();


        for (Map.Entry<ConsumerGroupMetdata,Long> e : sorted) {
            // This loop prints entries. You can use the same loop
            // to get the keys from entries, and add it to your target list.
            System.out.println("CG Name : "+ e.getKey().getConsumerGroupName()  + " , And Topic Name : "+ e.getKey().getTopicName()+" , has a Lag of : "+e.getValue());
            sortedOutput.add(e.getKey());
        }
        return sortedOutput;
    }
}

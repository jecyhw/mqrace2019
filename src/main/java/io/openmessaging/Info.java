package io.openmessaging;

public class Info {
    private int tid;
    private int tMaxInterval;
    private int uniqueTNum;
    private long lastT;
    private int[] tTInterValArr;
    private int[] tAInterValArr;
    static final int offset = 39000;

    public Info(int tid) {
        this.tid = tid;
        lastT = -1;
        uniqueTNum = 0;
        tMaxInterval = Integer.MIN_VALUE;
        tTInterValArr = new int[502];
        tAInterValArr = new int[40000];
    }

    public void cal(Message message){
        int tAInterval = (int)(message.getT() - message.getA())+offset;
        tAInterValArr[tAInterval]++;

        if(lastT != -1){
            int interval = (int)(message.getT() - lastT);
            if(interval > 500){
                tTInterValArr[501]++;
            }else{
                tTInterValArr[interval]++;
            }

            if (interval > tMaxInterval){
                tMaxInterval = interval;
            }
        }

        if (message.getT() != lastT){
            uniqueTNum++;
            lastT = message.getT();
        }

    }

    public void print(){
        System.out.println("[info T&A]---------------------------------------------------------------");
        System.out.println("tid:" + tid + ", tMaxInterval:"+tMaxInterval+", uniqueTNum:"+uniqueTNum+"");
        System.out.println("tTInterValArr:");
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < tTInterValArr.length; i++){
            sb.append(""+i+":" + tTInterValArr[i] + ", ");
        }
        System.out.println(sb.toString());
        System.out.println("tAInterValArr:");

        StringBuilder sb2 = new StringBuilder();
        for (int i = 0; i < tAInterValArr.length; i++){
            if(tAInterValArr[i] > 0){
                sb2.append(""+(i-offset)+":" + tAInterValArr[i] + ", ");
            }
        }
        System.out.println(sb2.toString());
    }
}

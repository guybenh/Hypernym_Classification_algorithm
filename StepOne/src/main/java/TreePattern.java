import org.apache.hadoop.io.Text;

import java.util.List;

public class TreePattern {

    private String pattern;
    private NounPair pair;

//(word1,CATEGORY1:RELATION:CATEGORY2, word2).
    public TreePattern(List<Node> path, int total_count) {
        Node source =path.get(0);
        Node target = path.get(path.size()-1);

        this.pair = new NounPair(new Text(source.getWord()) , new Text(target.getWord()), total_count);
        this.pattern = "";
        for(int i=0; i<path.size(); i++){
            if(i == path.size() -1){
                this.pattern += path.get(i).getPosTag();
            }
            else{
                this.pattern += path.get(i).getPosTag() +":"+path.get(i).getDepLabel()+":";
            }
        }
    }

    public String getPattern() {
        return pattern;
    }

    public NounPair getPair() {
        return pair;
    }
}

public class Node {

    private String word;
    private String posTag;
    private String depLabel;
    private int headIndex;

    public Node(String[] splitted) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < splitted.length - 3; i++) {
            sb.append(splitted[i]).append("/");
        }
        sb.setLength(sb.length() - 1);
        String to_stem = sb.toString();
        Stemmer stemmer = new Stemmer();
        stemmer.add(to_stem.toCharArray(), to_stem.length());
        stemmer.stem();
        this.word = stemmer.toString();
        this.posTag = splitted[splitted.length - 3].toLowerCase();
        this.depLabel = splitted[splitted.length - 2];
        this.headIndex = Integer.parseInt(splitted[splitted.length - 1]);
    }

    public String getWord() {
        return word;
    }

    public String getPosTag() {
        return posTag;
    }

    public String getDepLabel() {
        return depLabel;
    }

    public int getHeadIndex() {
        return headIndex;
    }

}

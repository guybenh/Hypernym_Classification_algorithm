import org.jgrapht.GraphPath;
import org.jgrapht.alg.shortestpath.DijkstraShortestPath;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.SimpleDirectedGraph;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class DependencyTree {

    private SimpleDirectedGraph<Node, DefaultEdge> tree;
    private int total_count;
    private List<TreePattern> patterns;

    public DependencyTree(String line) {
        String[] splitted = line.split("\t");
        List<Node> nodes = this.parseNodes(splitted[1]);
        this.total_count = Integer.parseInt(splitted[2]);
        tree = new SimpleDirectedGraph<>(DefaultEdge.class);
        buildTree(nodes);
        this.patterns = new ArrayList<>();
        findPatterns(nodes);
    }


    private void findPatterns(List<Node> nodes) {
        List<String> nounsList = Arrays.asList("nn", "nns", "nnp", "nnps");
        DijkstraShortestPath alg = new DijkstraShortestPath(this.tree);
        for (Node source : nodes) {
            if (!nounsList.contains(source.getPosTag()))
                continue;
            for (Node target : nodes) {
                if (source.equals(target) || !nounsList.contains(target.getPosTag()))
                    continue;
                GraphPath<Node, DefaultEdge> path = alg.getPath(source, target);
                if (path != null && path.getVertexList().size() > 1) {
                    patterns.add(new TreePattern(path.getVertexList(), this.total_count));
                }
            }
        }
    }

    private void buildTree(List<Node> nodes) {
        for (Node node : nodes) {
            tree.addVertex(node);
        }
        for (Node node : nodes) {
            if (node.getHeadIndex() > 0)
                tree.addEdge(nodes.get(node.getHeadIndex() - 1), node);
        }
    }

    private List<Node> parseNodes(String s) {
        List<Node> nodes = new ArrayList<>();
        String[] splitted = s.split(" ");
        for (String node : splitted) {
            String[] line_splitted = node.split("/");
            nodes.add(new Node(line_splitted));
        }
        return nodes;
    }

    public List<TreePattern> getPatterns() {
        return patterns;
    }
}

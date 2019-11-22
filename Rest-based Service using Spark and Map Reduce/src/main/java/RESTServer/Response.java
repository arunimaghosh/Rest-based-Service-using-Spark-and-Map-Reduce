package RESTServer;

public class Response {
	
    private final long id;
    private final ExecResult MapReduce;
    private final ExecResult Spark;

    public Response(long id, ExecResult MapReduce, ExecResult Spark) {
        this.id = id;
        this.MapReduce = MapReduce;
        this.Spark = Spark;
    }

    public long getId() {
        return id;
    }
    
    public ExecResult getMapReduce() {
        return MapReduce;
    }

    public ExecResult getSpark() {
        return Spark;
    }

}

package RESTServer;

public class ExecResult {
	
    private final long TimeTaken;
    private final String[] ExecutionDetails;
    private final String[] Result;

    public ExecResult(long TimeTaken, String[] Result, String[] ExecutionDetails) {
        this.TimeTaken = TimeTaken;
        this.Result = Result;
		this.ExecutionDetails = ExecutionDetails;
    }

    public long getTimeTaken() {
        return TimeTaken;
    }

    public String[] getExecutionDetails() {
        return ExecutionDetails;
    }
    
    public String[] getResult() {
        return Result;
    }

}

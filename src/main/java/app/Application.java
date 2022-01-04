package app;

public interface Application {

    void executeOperation(byte[] op, boolean local, long instId);
    void installState(byte[] state);

    byte[] getSnapshot();
}

package app;

public interface Application {

    void executeOperation(byte[] op, boolean local);
    void installState(byte[] state);

    byte[] getState();
}

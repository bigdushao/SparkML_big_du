package org.du.rpc;

import java.io.IOException;

/**
 * Created by dushao on 18-1-25.
 * @author dushao
 * @date 2018/01/25
 */
public interface Server {
    public void stop();

    public void start() throws IOException;

    public void register(Class ServiceInterface, Class impl);

    public boolean isRunning();

    public int getPort();
}

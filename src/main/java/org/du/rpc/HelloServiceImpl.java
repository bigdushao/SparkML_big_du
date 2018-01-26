package org.du.rpc;

/**
 * Created by dushao on 18-1-25.
 */
public class HelloServiceImpl implements HelloService {
    public String sayHi(String name){
        return "Hi, " + name;
    }
}

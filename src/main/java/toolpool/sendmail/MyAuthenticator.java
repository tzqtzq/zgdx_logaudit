package toolpool.sendmail;

import javax.mail.*;

/**
 * Created by tianzhongqiu on 2018/6/28.
 */
public class MyAuthenticator extends Authenticator {


    String userName=null;
    String password=null;

    public MyAuthenticator(){
    }
    public MyAuthenticator(String username, String password) {
        this.userName = username;
        this.password = password;
    }
    protected PasswordAuthentication getPasswordAuthentication(){
        return new PasswordAuthentication(userName, password);
    }

}

package toolpool.sendmail;

/**
 * Created by tianzhongqiu on 2018/6/28.
 */
public class MainSender {

    public void sendEmail(String contents){

        MailSenderInfo mailInfo = new MailSenderInfo();
        mailInfo.setMailServerHost("smtp.qq.com");
        mailInfo.setMailServerPort("25");
        mailInfo.setValidate(true);
        mailInfo.setUserName("971314085@qq.com");
        mailInfo.setPassword("brdskfygdqzobbbf");// 您的邮箱密码
        mailInfo.setFromAddress("971314085@qq.com");
        mailInfo.setToAddress("tzq_engineer@163.com");


        mailInfo.setSubject("底层组件审计日志实时监控平台");
        mailInfo.setContent(contents);


        // SimpleMailSender 这个类主要来发送邮件
        SimpleMailSender.sendTextMail(mailInfo);// 发送文体格式

        //SimpleMailSender.sendHtmlMail(mailInfo);// 发送html格式

    }

    public static void main(String[] args) {
        new MainSender().sendEmail("成功了，啦啦啦啦啦啦啦，啦啦啦啦啦啦，根本没在怕....");
    }

}

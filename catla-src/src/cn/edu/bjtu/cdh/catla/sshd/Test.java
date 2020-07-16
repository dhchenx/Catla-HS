package cn.edu.bjtu.cdh.catla.sshd;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;

import org.apache.sshd.client.SshClient;
import org.apache.sshd.client.channel.ChannelExec;
import org.apache.sshd.client.channel.ClientChannel;
import org.apache.sshd.client.channel.ClientChannelEvent;
import org.apache.sshd.client.future.AuthFuture;
import org.apache.sshd.client.future.ConnectFuture;
import org.apache.sshd.client.session.ClientSession;

public class Test {
	public static void main(String[] args) throws IOException, InterruptedException {
		String cmd = "cd /usr/hadoop && ls";
		SshClient client = SshClient.setUpDefaultClient();
		client.start();
		ConnectFuture connect = client.connect("hadoop", "192.168.159.132", 22);
		boolean issuccess = connect.await(5000L);
		if (issuccess) {
			ClientSession session = connect.getSession();
			session.addPasswordIdentity("Passw0rd");
			// session.addPublicKeyIdentity(SecurityUtils.loadKeyPairIdentity("keyname", new
			// FileInputStream("priKey.pem"), null));
			
			AuthFuture auth = session.auth();
			auth.await();
			if (!auth.isSuccess())
				System.out.println("auth failed");

			ChannelExec ec = session.createExecChannel(cmd);
			
			ByteArrayOutputStream errs = new ByteArrayOutputStream();
			
			ByteArrayOutputStream outs = new ByteArrayOutputStream();
			ec.setOut(outs);
			ec.setErr(errs);
			
			ec.open();
			
			ec.waitFor(Arrays.asList(ClientChannelEvent.CLOSED), 0);
			
			ec.close();
			client.stop();

			String result = new String(outs.toByteArray(), "utf-8");
			String err_result=new String(errs.toByteArray(), "utf-8");
			
			System.out.println("results=");
			System.out.println(result);
			System.out.println("errs=");
			System.out.println(err_result);

		}
	}
}

package ui;

import java.awt.BorderLayout;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import javax.swing.JComboBox;
import javax.swing.JFrame;
import javax.swing.JScrollPane;
import javax.swing.JTable;
import javax.swing.table.DefaultTableModel;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.ChannelExec;

public class SwingForRemoteAccess {

	private JFrame frame1;
	String[] args1 = new String[3];

	public static void main(String[] args) {
		try {
			SwingForRemoteAccess window = new SwingForRemoteAccess();
			window.frame1.setVisible(true);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public SwingForRemoteAccess() {
		initialize();
	}

	javax.swing.JPanel JPanel1 = new javax.swing.JPanel();
	javax.swing.JButton JButtonExecute = new javax.swing.JButton();
	javax.swing.JTextField JInputText = new javax.swing.JTextField();
	javax.swing.JTextField JOutputText = new javax.swing.JTextField();
	javax.swing.JLabel JInputLabel = new javax.swing.JLabel();
	javax.swing.JLabel JOutputLabel = new javax.swing.JLabel();

	javax.swing.JTextField JUrlText = new javax.swing.JTextField();
	javax.swing.JLabel JUrlLabel = new javax.swing.JLabel("Url");
	javax.swing.JTextField JJarLocationText = new javax.swing.JTextField();
	javax.swing.JLabel JJarLocationLabel = new javax.swing.JLabel(
			"Jar Location");

	javax.swing.JTextField JMainText = new javax.swing.JTextField();
	javax.swing.JLabel JMainLabel = new javax.swing.JLabel("Main Class");

	javax.swing.JTextField JJarText = new javax.swing.JTextField();
	javax.swing.JLabel JJarLabel = new javax.swing.JLabel("JarName");

	JComboBox<String> JcomboBox = new JComboBox<String>();

	JComboBox<String> JcomboBoxCommand = new JComboBox<String>();

	private void initialize() {
		JcomboBox.addItem("Cloudera");
		JcomboBox.addItem("HortonWorks");
		JcomboBoxCommand.addItem("pair");
		JcomboBoxCommand.addItem("stripe");
		JcomboBoxCommand.addItem("hybrid");
		frame1 = new JFrame();
		frame1.setTitle("Remote Hdfs Connector");
		frame1.setDefaultCloseOperation(javax.swing.JFrame.DO_NOTHING_ON_CLOSE);
		frame1.getContentPane().setLayout(null);
		frame1.setSize(600, 700);
		frame1.setVisible(false);

		int x = 0;
		int y = 30;
		JcomboBox.setVisible(true);
		frame1.getContentPane().add(JcomboBox);
		JcomboBox.setBounds(78, x += y, 160, 30);

		JcomboBoxCommand.setVisible(true);
		frame1.getContentPane().add(JcomboBoxCommand);
		JcomboBoxCommand.setBounds(78, x += y + 10, 160, 30);

		JUrlLabel.setVisible(true);
		JUrlLabel.setText("Url");
		frame1.getContentPane().add(JUrlLabel);
		JUrlLabel.setBounds(78, x += y, 160, 30);

		JUrlText.setVisible(true);
		JUrlText.setText("192.168.56.101");
		frame1.getContentPane().add(JUrlText);
		JUrlText.setBounds(78, x += y, 160, 30);

		JJarLabel.setVisible(true);
		JJarLabel.setText("Jar File Location");
		frame1.getContentPane().add(JJarLabel);
		JJarLabel.setBounds(78, x += y, 160, 30);

		JJarText.setVisible(true);
		JJarText.setText("/home/cloudera/map-reduce1-0.0.1-SNAPSHOT");
		frame1.getContentPane().add(JJarText);
		JJarText.setBounds(78, x += y, 160, 30);

		JMainLabel.setVisible(true);
		JMainLabel.setText("Main Class");
		frame1.getContentPane().add(JMainLabel);
		JMainLabel.setBounds(78, x += y, 160, 30);

		JMainText.setVisible(true);
		JMainText.setText("main.MainClass");
		frame1.getContentPane().add(JMainText);
		JMainText.setBounds(78, x += y, 160, 30);

		JInputLabel.setVisible(true);
		JInputLabel.setText("Input");
		frame1.getContentPane().add(JInputLabel);
		JInputLabel.setBounds(78, x += y, 160, 30);

		JInputText.setVisible(true);
		JInputText.setText("/user/cloudera/inputdir");
		frame1.getContentPane().add(JInputText);
		JInputText.setBounds(78, x += y, 160, 30);

		JOutputLabel.setVisible(true);
		JOutputLabel.setText("Output");
		frame1.getContentPane().add(JOutputLabel);
		JOutputLabel.setBounds(78, x += y, 160, 30);

		JOutputText.setVisible(true);
		JOutputText.setText("/user/cloudera/output");
		frame1.getContentPane().add(JOutputText);
		JOutputText.setBounds(78, x += y, 160, 30);

		JButtonExecute.setText("Connect");
		frame1.getContentPane().add(JButtonExecute);
		JButtonExecute.setBounds(78, x += y + 10, 160, 30);

		JPanel1.setLayout(null);
		frame1.getContentPane().add(BorderLayout.CENTER, JPanel1);
		JPanel1.setBounds(250, 30, 200, 500);
		JScrollPane1 = new JScrollPane();
		JPanel1.add(JScrollPane1);
		JScrollPane1.setBounds(250, 30, 200, 400);
		JScrollPane1.getViewport().add(JTable1);
		JTable1 = new JTable();
		frame1.getContentPane().add(JTable1);
		JTable1.setBounds(250, 30, 200, 500);

		SymWindow aSymWindow = new SymWindow();
		frame1.addWindowListener(aSymWindow);
		SymAction lSymAction = new SymAction();
		JButtonExecute.addActionListener(lSymAction);
	}

	class SymAction implements java.awt.event.ActionListener {
		public void actionPerformed(java.awt.event.ActionEvent event) {
			Object object = event.getSource();
			if (object == JButtonExecute)
				JButtonExecute_actionPerformed(event);
		}

	}

	class SymWindow extends java.awt.event.WindowAdapter {
		public void windowClosing(java.awt.event.WindowEvent event) {

			Object object = event.getSource();
			if (object == frame1) {
				windowClosing1(event);
			}
		}
	}

	void JButtonExecute_actionPerformed(java.awt.event.ActionEvent event) {
		RunHadoopCommand(getCommand(JcomboBoxCommand.getSelectedItem()
				.toString()));
		
	}

	String getCommand(String commandtype) {
		String inputdir = JInputText.getText();
		String output = JOutputText.getText();
		String mainclass = JMainText.getText();
		String jarfilename = JJarText.getText();
		String Command = "hadoop  jar " + jarfilename + ".jar " + mainclass
				+ " " + " " + inputdir + " " + output + " " + commandtype;
		System.out.println(Command);
		return Command;
	}

	void windowClosing1(java.awt.event.WindowEvent event) {
		// Object object = event.getSource();
		exitApplication();
	}

	void exitApplication() {
		try {
			frame1.setVisible(false); // hide the Frame
			frame1.dispose(); // free the system resources
			System.exit(0); // close the application
		} catch (Exception e) {
		}
	}

	void RunHadoopCommand(String command) {
		args1[0] = JInputText.getText();
		args1[1] = JOutputText.getText();
		args1[2] = JUrlText.getText();
		System.out.println(args1[0] + "     " + args1[1] + "    " + args1[2]);

		String host = args1[2];
		String user = "cloudera";
		String password = "cloudera";
		String command1 = command;
		try {

			java.util.Properties config = new java.util.Properties();
			config.put("StrictHostKeyChecking", "no");
			JSch jsch = new JSch();
			Session session = jsch.getSession(user, host, 22);
			session.setPassword(password);
			session.setConfig(config);
			session.connect();
			System.out.println("Connected");

			Channel channel = session.openChannel("exec");
			((ChannelExec) channel).setCommand(command1);
			channel.setInputStream(null);
			((ChannelExec) channel).setErrStream(System.err);

			InputStream in = channel.getInputStream();
			channel.connect();
			byte[] tmp = new byte[1024];
			while (true) {
				while (in.available() > 0) {
					int i = in.read(tmp, 0, 1024);
					if (i < 0)
						break;
					System.out.print(new String(tmp, 0, i));
				}
				if (channel.isClosed()) {
					System.out.println("exit-status: "
							+ channel.getExitStatus());
					break;
				}
				try {
					Thread.sleep(1000);
				} catch (Exception ee) {
				}
			}
			channel.disconnect();
			session.disconnect();
			System.out.println("DONE");
			GetFileFromHdfs();
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	private Object rowdata[];
	javax.swing.JTable JTable1 = new javax.swing.JTable();
	private JScrollPane JScrollPane1;

	void GetFileFromHdfs() {
	 
		System.setProperty("HADOOP_USER_NAME", "cloudera");
		try {
			Path pt = new Path("hdfs://" + JUrlText.getText() + ":9000"
					+ JOutputText.getText() + "/part-r-00000");
			FileSystem fs = FileSystem.get(new Configuration());
			BufferedReader br = new BufferedReader(new InputStreamReader(
					fs.open(pt)));
			String line;
			line = br.readLine();
			System.out.println(pt);
			while (line != null) {
				System.out.println(line);
				line = br.readLine();
			}
//			DefaultTableModel model = new DefaultTableModel();
//			model.addColumn("Key");
//			model.addColumn("Value");
//			rowdata = new Object[2];
//			while ((str = bfr.readLine()) != null) {
//				String[] allIds = str.split(" ");
//				if (allIds.length < 2) {
//					rowdata[0] = allIds[0];
//					rowdata[1] = "";
//				} else {
//					rowdata[0] = allIds[0];
//					rowdata[1] = allIds[1];
//				}
//
//				model.addRow(rowdata);
//				System.out.println(str);
//			}
//
//			JTable1.setModel(model);

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.out.println(e.getMessage());
		}
	}
}

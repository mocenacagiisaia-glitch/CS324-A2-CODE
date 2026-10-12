package edu.usp.cs324.client;

import edu.usp.cs324.api.Job;
import java.awt.*;
import java.nio.file.Path;
import javax.swing.*;
import javax.swing.table.DefaultTableModel;

/** Each submission owns a SwingWorker; all file/network/calculation waits stay off the EDT. */
public final class ClientGui extends JFrame {
    private final JTextField host = new JTextField("127.0.0.1", 14);
    private final JTextField port = new JTextField("1099", 6);
    private final JComboBox<Job.Type> type = new JComboBox<>(Job.Type.values());
    private final JTextArea input = new JTextArea("1,2,3,4,5", 5, 50);
    private final DefaultTableModel results = new DefaultTableModel(new String[]{"Task", "Type", "Status", "Result / error"}, 0) {
        @Override public boolean isCellEditable(int row, int column) { return false; }
    };

    public ClientGui(String bootstrapHost, String bootstrapPort) {
        super("DistriLab jobs");
        host.setText(bootstrapHost);
        port.setText(bootstrapPort);
        JPanel connection = new JPanel();
        connection.add(new JLabel("Bootstrap host")); connection.add(host);
        connection.add(new JLabel("Port")); connection.add(port); connection.add(type);
        JPanel actions = new JPanel();
        JButton manual = new JButton("Submit entered data");
        JButton csv = new JButton("Submit CSV file...");
        actions.add(manual); actions.add(csv);
        manual.addActionListener(event -> submit(null));
        csv.addActionListener(event -> {
            JFileChooser chooser = new JFileChooser();
            if (chooser.showOpenDialog(this) == JFileChooser.APPROVE_OPTION)
                submit(chooser.getSelectedFile().toPath());
        });
        JPanel editor = new JPanel(new BorderLayout(8, 8));
        editor.add(connection, BorderLayout.NORTH);
        editor.add(new JScrollPane(input), BorderLayout.CENTER);
        JPanel bottom = new JPanel(new BorderLayout());
        bottom.add(new JLabel("Headerless integers separated by commas/newlines; PRIMESUM: start,end (inclusive)."), BorderLayout.NORTH);
        bottom.add(actions, BorderLayout.SOUTH);
        editor.add(bottom, BorderLayout.SOUTH);
        JTable table = new JTable(results);
        table.setAutoResizeMode(JTable.AUTO_RESIZE_LAST_COLUMN);
        add(editor, BorderLayout.NORTH); add(new JScrollPane(table), BorderLayout.CENTER);
        setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        setSize(900, 500); setLocationByPlatform(true);
    }

    private void submit(Path file) {
        String endpoint = host.getText().strip();
        String portText = port.getText().strip();
        String data = input.getText();
        Job.Type selected = (Job.Type) type.getSelectedItem();
        int row = results.getRowCount();
        results.addRow(new Object[]{row + 1, selected, "Running", file == null ? "Manual input" : file.toString()});
        new SwingWorker<String, Void>() {
            @Override protected String doInBackground() throws Exception {
                int number = Integer.parseInt(portText);
                if (number < 1 || number > 65535) throw new IllegalArgumentException("Port must be 1..65535");
                Job job = file == null ? JobInput.parse(selected, data) : JobInput.read(selected, file);
                return ClusterClient.connect(endpoint, number).submit(job).toString();
            }
            @Override protected void done() {
                try {
                    results.setValueAt(get(), row, 3); results.setValueAt("Complete", row, 2);
                } catch (Exception e) {
                    Throwable failure = e.getCause() == null ? e : e.getCause();
                    results.setValueAt(failure.toString(), row, 3); results.setValueAt("Failed", row, 2);
                }
            }
        }.execute();
    }

    public static void main(String[] args) {
        if (args.length != 0 && args.length != 2)
            throw new IllegalArgumentException("ClientGui [bootstrapHost bootstrapPort]");
        SwingUtilities.invokeLater(() -> new ClientGui(args.length == 0 ? "127.0.0.1" : args[0],
                args.length == 0 ? "1099" : args[1]).setVisible(true));
    }
}

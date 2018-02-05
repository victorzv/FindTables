import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.nio.file.*;
import java.sql.*;
import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Main {

    static final Logger logger = Logger.getLogger(Main.class);

    public static Connection con = null;

    public static void initConnection()  {
        System.out.println("Begin load driver");
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            System.out.println("Begin connect");
            con = DriverManager.getConnection("jdbc:mysql://localhost/TablesIn?useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC&autoReconnect=true&useSSL=false", "user", "user");
            System.out.println(con);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void insertToFiles(String file){
        try {
            PreparedStatement pstmt = con.prepareStatement("INSERT INTO CentrexFile(filename) VALUES(?)");
            pstmt.setString(1, file);
            long affectedrows = pstmt.executeUpdate();
            if (affectedrows == 0){
                throw  new SQLException("Can not insert into database " + file);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void insertToTables(String tables){
        try {
            PreparedStatement pstmt = con.prepareStatement("INSERT INTO TablesDB(tablename) VALUES(?)");
            pstmt.setString(1, tables);
            long affectedrows = pstmt.executeUpdate();
            if (affectedrows == 0){
                throw new SQLException("Can not insert into TablesDB " + tables);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

    public static Long getIdFiles(String file){
        Long id = -1L;
        try {
            PreparedStatement pstmt = con.prepareStatement("SELECT id_file FROM CentrexFile WHERE filename=?");
            pstmt.setString(1, file);

            ResultSet rs = pstmt.executeQuery();
            while (rs.next()){
                id = rs.getLong(1);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return id;
    }

    public static Long getIdTables(String table){
        Long id = -1L;
        try {
            PreparedStatement pstmt = con.prepareStatement("SELECT id_table FROM TablesDB WHERE tablename=?");
            pstmt.setString(1, table);

            ResultSet rs = pstmt.executeQuery();
            while (rs.next()){
                id = rs.getLong(1);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return id;
    }

    public static void insertInToFT(Long id_file, Long id_table){
        try {
            if (getIdCommon(id_file, id_table) != -1L) return;
            PreparedStatement pstmt = con.prepareStatement("INSERT INTO FilesTables(id_file, id_table) VALUES(?, ?)");
            pstmt.setLong(1, id_file);
            pstmt.setLong(2, id_table);
            long affectedrows = pstmt.executeUpdate();
            if (affectedrows == 0){
                throw new SQLException("Can not insert into FilesTables");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static Long getIdCommon(Long id_file, Long id_table){
        Long result = -1L;
        try {
            PreparedStatement pstmt = con.prepareStatement("SELECT id FROM FilesTables WHERE id_file=? AND id_table=?");
            pstmt.setLong(1, id_file);
            pstmt.setLong(2, id_table);
            ResultSet rs = pstmt.executeQuery();
            while (rs.next()){
                result = rs.getLong(1);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return result;
    }

    public static void doDatabse(String pathargc) throws Exception{
        initConnection();
		// table names from database
        String[] tableNames = {"Acl", "Activity", "ActivityBackup", "Alarm", "Alias", "AliasGroup", "Attendant", "AttendantCommand",
                "AuthCallerId", "BWList", "CP_Prompts", "Call", "CallAdvertisingPIN", "CallAdvertisingPrompts", "CallQueueCallsStatistic", "CallQueueReception",
                "CallbackOrder", "Category", "Codec", "CodecGroup", "Conference", "Configuration", "CpcCode", "CpcName",
                "CpcNamespace", "CpcType", "CustomCategory", "CustomPrompt", "Dependence", "DisconnectCodes", "DisconnectCodesNamespace", "Domain",
                "EMailMap", "Enum", "ExternalRegistrar", "Filter", "FollowMe", "FollowMeBackup", "FollowMeScripts", "FollowMeScriptsBackup",
                "Forwarding", "Gateway", "Group", "IP", "IPMobileRequest", "IPOnEvents", "IVRScript", "MvtsConfig",
                "Operator", "Option", "PersonalDetails", "Pincode", "PollerConfig", "PollingState", "PortMap", "ProfileInfo",
                "Prompt", "RadiusPrompt", "Registrar", "Route", "RouteTemp", "SMTP", "Service", "SimpleBillingInfo",
                "SpeedDial", "Status", "Switch", "SystemStatistic", "Translation", "User", "UserCommandsOnEvents",
                "UserDataMobileRequest", "UserTemplate", "Versions", "VmsSettings", "VotePhone"};

//        Arrays.asList(tableNames).stream().forEach(p->insertToTables(p));
        System.out.println(Arrays.asList(tableNames).stream().distinct().count());

        String path = pathargc;

        Files.walk(Paths.get(path))
                .map(p -> p.toFile())
                .filter(p -> p.getName().endsWith(".cs"))
                .forEach(p -> {
                    String fileContent = "";
                    try {
                        fileContent = new String(Files.readAllBytes(Paths.get(p.getAbsolutePath())));
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    for (String tname : tableNames) {
                        if (fileContent.indexOf((" " + tname + " ")) >= 0) {
                            Long id_file = getIdFiles(p.getAbsolutePath());
                            if (id_file == -1L) {
                                insertToFiles(p.getAbsolutePath());
                                id_file = getIdFiles(p.getAbsolutePath());
                            }
                            Long id_table = getIdTables(tname);
                            if (id_table == -1L) {
                                insertToTables(tname);
                                id_table = getIdTables(tname);
                            }
                            Long id_common = getIdCommon(id_file, id_table);
                            if (id_common == -1L){
                                insertInToFT(id_file, id_table);
                                System.out.println("file: " + p.getAbsolutePath() + " and table: " + tname);
                            }
                        }
                    }
                });

    }

    public static void main(String[] args) throws Exception{

        BasicConfigurator.configure();

		
		// This code find regex in the files with extention (without mask *) in the path
		// If you need found tables in cs - you should comment code above in the main and call doDatabse function
		
        if (args.length == 0){
            logger.error("You should lanch programm in next format command line:");
            logger.error("java -jar programm path extention regex");
            return;
        }

        String path = args[0];
        String ext = "." + args[1];

        Pattern pattern = Pattern.compile(args[2]);

        if (!Files.exists(Paths.get(path))){
            logger.error("the path: " + path + " doesn't exist!");
            return;
        }

        Files.walk(Paths.get(path))
                .map(p->p.toFile())
                .filter(f->f.getName().endsWith(ext))
                .forEach(f->{
                    String fileContent = "";
                    try {
                        fileContent = new String(Files.readAllBytes(Paths.get(f.getAbsolutePath())));
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    Matcher matcher = pattern.matcher(fileContent);
                    int counter = 0;
                    while (matcher.find()) {
                        counter++;
                    }
                    if (counter != 0){
                        logger.info("File: " + f.getAbsolutePath() + " has " + counter + " matches");
                    }
                });
    }
}

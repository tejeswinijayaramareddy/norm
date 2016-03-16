package norm;
import norm.FindingAllSubSets;
import java.sql.*;
import java.util.*;
import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Iterator;

public class postgres {

    private static Statement st = null;

    public void connection(String SchemaFilePath) throws SQLException {

        File filesql = new File(System.getProperty("user.home"), "Desktop/SqlQueries.sql");
        if (filesql.exists()) {
            filesql.delete();
        }

        File file = new File(System.getProperty("user.home"), "Desktop/Output.txt");
        if (file.exists()) {
            file.delete();
        }

        // Load JDBC driver
        try {
            //Class.forName("com.vertica.jdbc.Driver");
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException e) {
            // Could not find the driver class. Likely an issue
            // with finding the .jar file.
            System.err.println("Could not find the JDBC driver class.");
            e.printStackTrace();
            return;
        }

        // Create property object to hold username & password
        Properties myProp = new Properties();
        myProp.put("user", "postgres");
        myProp.put("password", "White@12345");
        //myProp.put("user", "cosc6340");
        //myProp.put("password", "1pmMon-Wed");
        Connection conn;
        try {
            //conn = DriverManager.getConnection("jdbc:vertica://129.7.242.19:5433/cosc6340", myProp);
            conn = DriverManager.getConnection("jdbc:postgresql://localhost:5433/cosc6340", myProp);
        } catch (SQLException e) {
            // Could not connect to database.
            System.err.println("Could not connect to database.");
            e.printStackTrace();
            return;
        }

        try {
            String table = new String();
            BufferedReader br = null;
            br = new BufferedReader(new FileReader(SchemaFilePath));
            String line;
            char C;

            while ((line = br.readLine()) != null) {
                Boolean checkNF = true;
                table: for (int i = 0; i < line.length(); i++) {
                    C = line.charAt(i);
                    if (C == '(') {
                        break table;
                    }
                    table = table + (String.valueOf(line.charAt(i)));
                }

                DatabaseMetaData dbm = conn.getMetaData();
                ResultSet rse = dbm.getTables(null, null, table.toLowerCase(), null);

                if (rse.next()) {
                    System.out.println("Checking for input schema in data base");
                    System.out.println("\n Table" + table + " Exists in the database");

                    line = line.replace(table, "");
                    line = line.substring(1, line.length() - 1);
                    String[] columns = line.split(",");
                    String col = "";

                    for (String column : columns) {
                        if (column.contains("(k)")) {
                            col = column.replace("(k)", "");
                        } else {
                            col = column;
                        }
                        ResultSet colmns = dbm.getColumns(null, null, table.toLowerCase(), col.toLowerCase());

                        if (colmns.next()) {
                            System.out.println(" column: " +col + " exists " + "in table: " +table + "");
                        } else {
                            GenerateOutputFile(" Column: " + col + " doesn't exist in table: " + table + " - Schema Incorrect","") ;
                            checkNF = false;
                        }

                    }

                    if(checkNF) {
                        if(CheckingIfTablehasRecords(table, conn)){
                            if (CheckFirstNormalForm(table, columns, conn)) {
                                if (CheckSecondNormalForm(table, columns, conn)) {
                                    CheckThirdNormalForm(table, columns, conn);
                                }
                            }
                        }
                    }
                } else {
                    GenerateOutputFile("Table " + table + " does not exist - Schema Incorrect","");

                }


                table = "";

            }
            br.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private boolean CheckingIfTablehasRecords(String table, Connection conn) throws SQLException, IOException {

        StringBuilder query = new StringBuilder();

        query.append("select count(*) from " + table);

        st = conn.createStatement();
        GenerateSqlFile(query.toString());
        ResultSet rs = executeQuery(query.toString());
        boolean HasRecords = false;

        if(rs.next()) {
            if(rs.getInt(1) > 0) {
                HasRecords = true;
            }
        }

        return HasRecords;
    }

    private static boolean CheckSecondNormalForm(String table, String[] columns, Connection conn)
            throws SQLException, IOException {
        // TODO Auto-generated method stub

		/* Declare an array of Keys */

        ArrayList<String> Keys = new ArrayList<String>();
        int countOfKeys = 0;
        boolean PartialDependency = false;
        int dep = 1;
        String Nonkey = "";
        String key = "";
        /* Declare an array of Non-Key Attributes */
        int countOfNonKeyAttr = 0;
        ArrayList<String> NonKeyAttr = new ArrayList<String>();
        System.out.println("\nChecking for 2 NF\n");
        for (String column : columns) {
            if (column.contains("(k)")) {
                Keys.add("" + column.replace("(k)", ""));
                key = key + column.replace("(k)", "");
                countOfKeys++;
            } else {
                NonKeyAttr.add(column);
                countOfNonKeyAttr++;}
        }

        List<String> set = Keys;
        FindingAllSubSets<String> it = new FindingAllSubSets<String>(set);
        ArrayList<String> KeysSubSet = new ArrayList<String>();
        while (it.hasNext()) {
            KeysSubSet.add(it.next().toString());
        }
        KeysSubSet.remove(0);
        //Collections.sort(KeysSubSet);
        List<String> FoundKeys = new ArrayList<>();
        List<String> KeyDependentNonKeys = new ArrayList<>();
        HashMap hmap = new HashMap();


        for(String ColumnLeft : KeysSubSet) {
            ArrayList<String> CopyNonKeys = NonKeyAttr;
            String[] SplitColumnLeft = ColumnLeft.replace("[", "").replace("]", "").replace(" ","").split(",");
            //String WhereLHSClause = "";
            int index = 0;
            StringBuilder WhereLHSClause = new StringBuilder("");
            ArrayList<String> FunctionalD = new ArrayList<String>();

            for (int i = 0; i < SplitColumnLeft.length; i++) {
                //	CopyNonKeys.remove(SplitColumnLeft[i]);
                if (i == 0) {
                    WhereLHSClause.append("( Copyone." + SplitColumnLeft[i] + " = CopyTwo." + SplitColumnLeft[i]+ ")");
                } else {
                    WhereLHSClause.append(" and (Copyone." + SplitColumnLeft[i] + " = CopyTwo." + SplitColumnLeft[i]+ ")");
                }

                FunctionalD.add(SplitColumnLeft[i]);
            }
            //System.out.println(WhereLHSClause);

            boolean PartialDep = false;

            st = conn.createStatement();


            for(int i = 0; i < CopyNonKeys.size(); i ++) {

                StringBuilder query = new StringBuilder();
                query.append("select count(*) ");
                query.append("from " + table + "  as Copyone, " + table + " as CopyTwo ");
                query.append("where" + WhereLHSClause + "");
                query.append(" and (Copyone." + CopyNonKeys.get(i) + " != CopyTwo." + CopyNonKeys.get(i) + ");");
                GenerateSqlFile(query.toString());
                ResultSet rsTransitive = executeQuery(query.toString());
                Nonkey = Nonkey +"," + CopyNonKeys.get(i);
                if ((rsTransitive.next())) {
                    int res = rsTransitive.getInt(1);
                    if (res != 0) {
                        System.out.println("no partial dependency:" + rsTransitive.getInt(1));

                    } else {

                        if(FunctionalD.size() < countOfKeys) {

                            PartialDependency = true;

							  /* start decomposition here*/
//								System.out.println();
                            for (int a = 0; a < FunctionalD.size(); a++) {
                                for (int b = 0; b < FoundKeys.size(); b++) {
                                    String lh = FunctionalD.get(a);
                                    String rh = FoundKeys.get(b);
                                    if (lh.equals(rh)&& (CopyNonKeys.get(i) == KeyDependentNonKeys.get(b))) {
                                        System.out.println("no partial depe" +
                                                "ndency:" + FunctionalD);
                                        PartialDependency = false;

                                    }
                                }
                            }


                            if (PartialDependency) {

                                FoundKeys.add(FunctionalD.toString().replace("[", "").replace("]", ""));

                                KeyDependentNonKeys.add(CopyNonKeys.get(i));
                                dep = 2;
                                System.out.println("***************Functional Dependency found is " + FunctionalD.toString().replace("[", "").replace("]", "") + " ---> " + CopyNonKeys.get(i));
                                System.out.println("Violated 2NF with dependency:" +FunctionalD.toString().replace("[", "").replace("]", "") + " ---> " + CopyNonKeys.get(i));
                                GenerateOutputFile(table + "     2NF     N      partial functional functional dependency; " + ""
                                        , "2NF");
                                GenerateOutputFile(FunctionalD.toString().replace("[", "").replace("]", "")+"-->" + CopyNonKeys.get(i), "2NF");
                                GenerateOutputFile(FunctionalD.toString().replace("[", "").replace("]", "")+"-->" + CopyNonKeys.get(i), "2NF");
                                //System.out.println("***"+FoundKeys+ ""+KeyDependentNonKeys );

                            }



                        }


                    }

                }
            }

            if(PartialDependency) {
                for (int k = 0; k < FoundKeys.size(); k++) {
                    if (hmap.containsKey(FoundKeys.get(k))) {
                        String temp = (String) hmap.get(FoundKeys.get(k));
                        temp = temp + "," + KeyDependentNonKeys.get(k);
                        hmap.put(FoundKeys.get(k), temp);
                    } else {
                        hmap.put(FoundKeys.get(k), KeyDependentNonKeys.get(k));
                    }

                }

                String str = GetEntryMap(hmap, table, conn);

            }


        }

		/* Time to Decompose*/

		/* check for repeated keys on left side*/


        if (dep == 1)
        {
            GenerateOutputFile(table + "     2NF     Y      No partial functional functional dependency Found; " + ""
                    , "2NF");
        }

        return true;
    }


    public static String GetEntryMap(Map hmap, String table, Connection conn)throws SQLException, IOException {
        Iterator it = hmap.entrySet().iterator();
        int i = 1;
        int j = 1;
        String str = "";
        String NK = "";
        while (it.hasNext()) {

            Map.Entry pair = (Map.Entry) it.next();
            System.out.println(pair.getKey() + " = " + pair.getValue());
            str.concat((String) pair.getKey());
            StringBuilder DecomposeQuery = new StringBuilder();
            DecomposeQuery.append("DROP TABLE IF EXISTS " + table + "_" + i + ";");
            st = conn.createStatement();
            GenerateSqlFile(DecomposeQuery.toString());
            st.executeUpdate(DecomposeQuery.toString());

            DecomposeQuery = new StringBuilder();
            DecomposeQuery.append("SELECT DISTINCT " + pair.getKey() + ", " + pair.getValue() + " INTO " + table + "_" + i + "  FROM " + table + ";");
            st = conn.createStatement();
            GenerateSqlFile(DecomposeQuery.toString());
            st.executeUpdate(DecomposeQuery.toString());
            str = str+pair.getKey();
            NK = NK + pair.getValue();

            i++;
        }

        GenerateOutputFile( "r1_1(" + str + "," + NK + ")" , "2NF");




        System.out.println("" + i);
        Iterator pt = hmap.entrySet().iterator();

        StringBuilder DecomposeQuery = new StringBuilder();
//		System.out.println(DropPair.getKey() + " = " + DropPair.getValue());
        DecomposeQuery.append("DROP TABLE IF EXISTS " + table + "fromorig ;");
//		DecomposeQuery = new StringBuilder();

        st = conn.createStatement();
        GenerateSqlFile(DecomposeQuery.toString());
        st.executeUpdate(DecomposeQuery.toString());

        DecomposeQuery = new StringBuilder();
        DecomposeQuery.append("SELECT  DISTINCT * INTO " + table + "fromorig FROM " + table + ";");

        st = conn.createStatement();
        GenerateSqlFile(DecomposeQuery.toString());
        st.executeUpdate(DecomposeQuery.toString());

        DecomposeQuery = new StringBuilder();
        DecomposeQuery.append("ALTER TABLE " + table + "fromorig  DROP COLUMN i;");


        st = conn.createStatement();
        GenerateSqlFile(DecomposeQuery.toString());
        st.executeUpdate(DecomposeQuery.toString());

        while (pt.hasNext()) {
            Map.Entry DropPair = (Map.Entry) pt.next();
            System.out.println(DropPair.getKey() + " = " + DropPair.getValue());

            DecomposeQuery = new StringBuilder();
            DecomposeQuery.append("ALTER TABLE " + table + "fromorig  DROP COLUMN " + DropPair.getValue() + "; ");

            st = conn.createStatement();
            GenerateSqlFile(DecomposeQuery.toString());
            st.executeUpdate(DecomposeQuery.toString());

            str = str.concat("" + DropPair.getKey());


            StringBuilder query = new StringBuilder();
            query = new StringBuilder();

            //System.out.println("*****************" +str);

            query.append("select * from (select count(*) from " + table + " ) as Count1 , (select count(*) from " + table + "fromorig as x join " +table+ "_" +(j) +" as y on x." + DropPair.getKey() + "= y." +DropPair.getKey() +") as Count2");
            st = conn.createStatement();
            GenerateSqlFile(query.toString());
            //st.executeUpdate(query.toString());



            ResultSet rsJoin = executeQuery(query.toString());
            j++;
            if (rsJoin.next()) {
                if (rsJoin.getInt(1) == rsJoin.getInt(2)) {
                    System.out.println(" Verification: Decomposition is lossless");
                    GenerateOutputFile("Verification: + join (r1_1, r1fromorig)? YES" + System.getProperty("line.separator"), "2NF" );
                } else {
                    System.out.println(" Verification: " + System.getProperty("line.separator")  +"something is a problem for lossless 2NFdecomposition");
                    GenerateOutputFile(" " + System.getProperty("line.separator")+ "join (r1_1, r1fromorig)? NO" + System.getProperty("line.separator"), "2NF" );
                }
            }

        }
        return str;
    }

    private static void CheckThirdNormalForm(String table, String[] columns, Connection conn) throws SQLException, IOException {

        ArrayList<String> Keys = new ArrayList<String>();

		/* Declare an array of Non-Key Attributes */
        ArrayList<String> NonKeyAttr = new ArrayList<String>();

        for (String column : columns) {
            if (column.contains("(k)")) {
                Keys.add("" + column.replace("(k)", ""));
            } else {
                NonKeyAttr.add(column);
            }
        }

        // FIRST CHECK WITH 1 SUBSET ON LEFT HAND SIDE
        boolean TransitiveDep = false;
        ArrayList<String> RemoveFromSet = new ArrayList<String>();
        if(NonKeyAttr.size() > 1){
            ArrayList<String> TobeRemoved = new ArrayList<String>();

            st = conn.createStatement();
            ArrayList<String> Dependent = new ArrayList<String>();
            for(int i = 0; i < NonKeyAttr.size(); i ++) {
                TransitiveDep = false;
                for (int j = i + 1; j < NonKeyAttr.size(); j++) {

                    StringBuilder query = new StringBuilder();

                    query.append("select count(*) ");
                    query.append("from " + table + "  as Copyone, " + table + " as CopyTwo ");
                    query.append("where Copyone." + NonKeyAttr.get(i) + " = CopyTwo." + NonKeyAttr.get(i));
                    query.append(" and Copyone." + NonKeyAttr.get(j) + " != CopyTwo." + NonKeyAttr.get(j));

                    GenerateSqlFile(query.toString());
                    ResultSet rsTransitive = executeQuery(query.toString());

                    if (rsTransitive.next()) {
                        if (rsTransitive.getInt(1) == 0) {
                            System.out.println("YES transitivite, do something");
                            Dependent.add(NonKeyAttr.get(j));
                            TransitiveDep = true;
                        }
                    }
                }
                if(TransitiveDep==true) {
                    TobeRemoved.add(NonKeyAttr.get(i));
                    String WhereLHSClause1 = "";
                    for (int z = 0; z < Dependent.size(); z++) {
                        if (z == 0) {
                            WhereLHSClause1 = Dependent.get(z);
                        } else {
                            WhereLHSClause1 += ", " + Dependent.get(z);
                        }
                    }

                    StringBuilder queryInsert = new StringBuilder();

                    queryInsert.append("DROP TABLE IF EXISTS " + table + "_temp3NF_A; ");
                    queryInsert.append("DROP TABLE IF EXISTS " + table + "_temp3NF_B; ");
                    queryInsert.append(System.getProperty("line.separator"));
                    queryInsert.append("SELECT " + NonKeyAttr.get(i) + ", " + WhereLHSClause1 + " INTO " + table + "_temp3NF_A  FROM " + table + " WHERE 1 = 2; ");
                    queryInsert.append("SELECT * INTO " + table + "_temp3NF_B  FROM " + table + " WHERE 1 = 2; ");
                    for (int w = 0; w < Dependent.size(); w++) {
                        queryInsert.append("ALTER TABLE " + table + "_temp3NF_B  DROP COLUMN " + Dependent.get(w) + "; ");
                    }
                    queryInsert.append("ALTER TABLE " + table + "_temp3NF_B  DROP COLUMN i;");

                    st = conn.createStatement();
                    GenerateSqlFile(queryInsert.toString());
                    st.executeUpdate(queryInsert.toString());

                    StringBuilder query = new StringBuilder();
                    query = new StringBuilder();

                    query.append("INSERT INTO " + table + "_temp3NF_A ( select " + NonKeyAttr.get(i) + ", " + WhereLHSClause1 +
                            " from " + table + " group by " + NonKeyAttr.get(i) + ", " + WhereLHSClause1 + "); ");
                    query.append("INSERT INTO " + table + "_temp3NF_B ( select " + Keys.toString().replace("[", "").replace("]", "") +
                            ", " + NonKeyAttr.toString().replace("[", "").replace("]", "").replace(", " +WhereLHSClause1, "") + " from " + table +
                            " group by " + Keys.toString().replace("[", "").replace("]", "") + ", " +
                            NonKeyAttr.toString().replace("[", "").replace("]", "").replace(", " + WhereLHSClause1, "") + ");");

                    GenerateSqlFile(query.toString());
                    st.executeUpdate(query.toString());

                    query = new StringBuilder();

                    query.append("select * from (select count(*) from " + table + " ) as Count1 , (select count(*) from " + table + "_temp3nf_b as b join " + table +
                            "_temp3nf_a as a " + " on a." + NonKeyAttr.get(i) + " = b." + NonKeyAttr.get(i) + ") as Count2");

                    GenerateSqlFile(query.toString());
                    ResultSet rsJoin = executeQuery(query.toString());


                    if (rsJoin.next()) {
                        if (rsJoin.getInt(1) == rsJoin.getInt(2)) {
                            TransitiveDep = true;
                            GenerateOutputFile(table + "     3NF     N      Not 3NF, transitive functional dependency; " +
                                            table + " decomposition: " + System.lineSeparator() +
                                            table + "_temp3nf_a: " + NonKeyAttr.get(i) + ", " + WhereLHSClause1 + System.lineSeparator() +
                                            table + "_temp3nf_b: " + Keys.toString().replace("[", "").replace("]", "") + ", " +
                                            NonKeyAttr.toString().replace(WhereLHSClause1, "").replace("[", "").replace("]", "").replace(", ,",", ") + System.lineSeparator() +
                                            "Verification: " + System.lineSeparator() +
                                            table + "= " + " join( " + table + "_temp3nf_a, " + table + "_temp3nf_b + )? YES " + System.lineSeparator()
                                    , "3NF");
                            RemoveFromSet.add(NonKeyAttr.get(i));
                            break;
                        }
                    }
                }

            }

            // CHECKING FOR SUBSETS OF TWO IN THE LEFT HAND SIDE

            if(TransitiveDep == false){

                ArrayList<String> CopyofNonKeys = new ArrayList<String>(NonKeyAttr);
                //ArrayList<String> CopyofNonKeys = NonKeyAttr;
                CopyofNonKeys.remove(CopyofNonKeys.size()-1);

                //Collections.sort(KeysSubSet, x);

                st = conn.createStatement();

                for(int i = 0; i < CopyofNonKeys.size(); i ++) {
                    for (int j = i + 1; j < CopyofNonKeys.size(); j++) {

                        StringBuilder query = new StringBuilder();

                        query.append("select count(*) ");
                        query.append("from " + table + "  as Copyone, " + table + " as CopyTwo ");
                        query.append("where Copyone." + CopyofNonKeys.get(i) + " = CopyTwo." + CopyofNonKeys.get(i));
                        query.append(" and Copyone." + CopyofNonKeys.get(j) + " = CopyTwo." + CopyofNonKeys.get(j));
                        query.append(" and Copyone." + NonKeyAttr.get(j+1) + " != CopyTwo." + NonKeyAttr.get(j+1));

                        GenerateSqlFile(query.toString());
                        ResultSet rsTransitive = executeQuery(query.toString());

                        if (rsTransitive.next()) {
                            if (rsTransitive.getInt(1) == 0) {
                                System.out.println("YES transitivite, do something");


                                StringBuilder queryInsert = new StringBuilder();

                                queryInsert.append("DROP TABLE IF EXISTS " + table + "_temp3NF_D; ");
                                queryInsert.append("DROP TABLE IF EXISTS " + table + "_temp3NF_E; ");
                                queryInsert.append(System.getProperty("line.separator"));
                                queryInsert.append("SELECT " + CopyofNonKeys.get(i) + ", " + CopyofNonKeys.get(j) + ", " +
                                        NonKeyAttr.get(j+1) + " INTO " + table + "_temp3NF_D  FROM " + table + " WHERE 1 = 2; ");
                                queryInsert.append("SELECT * INTO " + table + "_temp3NF_E  FROM " + table + " WHERE 1 = 2; ");
                                queryInsert.append("ALTER TABLE " + table + "_temp3NF_E  DROP COLUMN " + NonKeyAttr.get(j+1) + "; ");

                                queryInsert.append("ALTER TABLE " + table + "_temp3NF_E  DROP COLUMN i;");

                                st = conn.createStatement();
                                GenerateSqlFile(queryInsert.toString());
                                st.executeUpdate(queryInsert.toString());

                                StringBuilder queryTwoSubSet = new StringBuilder();
                                queryTwoSubSet = new StringBuilder();

                                queryTwoSubSet.append("INSERT INTO " + table + "_temp3NF_D ( select " + CopyofNonKeys.get(i) + ", " + CopyofNonKeys.get(j) + ", " +
                                        NonKeyAttr.get(j+1) + " from " + table + " group by " + CopyofNonKeys.get(i) + ", " + CopyofNonKeys.get(j) + ", " +
                                        NonKeyAttr.get(j+1) + "); ");
                                queryTwoSubSet.append("INSERT INTO " + table + "_temp3NF_E ( select " + Keys.toString().replace("[", "").replace("]", "") +
                                        ", " + NonKeyAttr.toString().replace("[", "").replace("]", "").replace(", " + NonKeyAttr.get(j+1), "") + " from " + table +
                                        " group by " + Keys.toString().replace("[", "").replace("]", "") + ", " +
                                        NonKeyAttr.toString().replace("[", "").replace("]", "").replace(", " + NonKeyAttr.get(j+1), "") + ");");

                                GenerateSqlFile(queryTwoSubSet.toString());
                                st.executeUpdate(queryTwoSubSet.toString());

                                queryTwoSubSet = new StringBuilder();

                                queryTwoSubSet.append("select * from (select count(*) from " + table + " ) as Count1 , (select count(*) from " + table + "_temp3nf_D as D " +
                                        "join " + table + "_temp3nf_E as E " +
                                        "on D." + CopyofNonKeys.get(i) + " = E." + CopyofNonKeys.get(i) + " and D." + CopyofNonKeys.get(j) + " = E." + CopyofNonKeys.get(j) +
                                        " ) as Count2");

                                GenerateSqlFile(queryTwoSubSet.toString());
                                ResultSet rsJoin = executeQuery(queryTwoSubSet.toString());


                                if (rsJoin.next()) {
                                    if (rsJoin.getInt(1) == rsJoin.getInt(2)) {
                                        TransitiveDep = true;
                                        GenerateOutputFile(table + "     3NF     N      Not 3NF, transitive functional dependency; " +
                                                        table + " decomposition: " + System.lineSeparator() +
                                                        table + "_temp3nf_D: " + CopyofNonKeys.get(i) + ", " + CopyofNonKeys.get(j) + ", " + NonKeyAttr.get(j+1) + System.lineSeparator() +
                                                        table + "_temp3nf_E:  " + Keys.toString().replace("[", "").replace("]", "") + ", " +
                                                        NonKeyAttr.toString().replace(NonKeyAttr.get(j+1), "").replace("[", "").replace("]", "").replace(", ,", ", ") + System.lineSeparator() +
                                                        "Verification: " + System.lineSeparator() +
                                                        table + "= " + " join( " + table + "_temp3nf_D, " + table + "_temp3nf_E + )? YES " + System.lineSeparator()
                                                , "3NF");
                                        RemoveFromSet.add(NonKeyAttr.get(i));
                                        break;
                                    }
                                }
                            }
                        }
                    }

                }

            }

            if(TransitiveDep == false){
                GenerateOutputFile(table + "     3NF     Y ", "3NF");
            }
        } else {
            GenerateOutputFile(table + "     3NF     Y ", "3NF");
        }
    }


    public static ResultSet executeQuery(String sqlStatement) {
        try {
            return st.executeQuery(sqlStatement);
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }
    }

    public static boolean CheckFirstNormalForm(String table, String[] columns, Connection conn)
            throws SQLException, IOException {

        String Keys = "";
        ArrayList<String> KeysNull = new ArrayList<String>();
        int countOfKeys = 0;

        System.out.println("\n\nChecking for first normal form!!");
        for (String column : columns) {
            if (column.contains("(k)")) {
                if (Keys == "") {
                    Keys = column.replace("(k)", "");
                } else {
                    Keys += ", " + column.replace("(k)", "");
                }
                KeysNull.add(column.replace("(k)", ""));
                countOfKeys++;
            }
        }

        // CHECKING FOR NULL IN PRIMARY KEY

        for (String CheckNull : KeysNull) {

            StringBuilder queryNull = new StringBuilder();
            String ResultsNull = "";

            queryNull.append("Select " + CheckNull + ", count(*) ");
            queryNull.append("From " + table + " ");
            queryNull.append("Where " + CheckNull + " is null ");
            queryNull.append("Group By " + CheckNull);

            st = conn.createStatement();
            GenerateSqlFile(queryNull.toString());
            ResultSet rsNull = executeQuery(queryNull.toString());

            if (rsNull.next()) {
                ResultsNull = table + "     1NF     N      Not 1NF, because of NULL value: Key: " + CheckNull;
                GenerateOutputFile(ResultsNull, "1NF");
                return false;
            }

            rsNull.close();
            st.close();

        }

        // CHECKING FOR DUPLICATES IN PRIMARY KEY

        StringBuilder query = new StringBuilder();
        String Results = "";

        query.append("Select " + Keys + ", count(*) ");
        query.append("From " + table + " ");
        query.append("Group by " + Keys + " ");
        query.append("Having count(*) > 1");

        st = conn.createStatement();
        GenerateSqlFile(query.toString());
        ResultSet rs = executeQuery(query.toString());

        while (rs.next()) {

            Results = table + "     1NF      N          Not 1NF, because of duplicate values: Keys: " + Keys + ": "
                    + ", value: " + rs.getObject(1) + ", quantity: " + rs.getObject(2);
            GenerateOutputFile(Results, "1NF");
            return false;
        }

        st.close();
        System.out.println(">> Table: " +table+ " is in 1NF!!");
        return true;

    }

    public static void GenerateSqlFile(String query) throws IOException {

        File file = new File(System.getProperty("user.home"), "Desktop/NF.sql");

        if (!file.exists()) {
            file.createNewFile();
        }

        PrintWriter pw = new PrintWriter(new FileWriter(file, true));
        pw.println("");
        pw.println(query);
        pw.close();

    }

    public static void GenerateOutputFile(String results, String option) throws IOException {

        File file = new File(System.getProperty("user.home"), "Desktop/NF.txt");

        if (!file.exists()) {
            file.createNewFile();
            PrintWriter pw = new PrintWriter(new FileWriter(file, true));
            pw.println("Table  Form    Complies     Explanation");
            pw.println(" ");
            pw.print(results);
            pw.println(" ");
            pw.close();
        } else {
            PrintWriter pw = new PrintWriter(new FileWriter(file, true));
            pw.print(results);
            pw.println(" ");
            pw.close();
        }

    }

    public static String[] GenerateKeysSubSets(ArrayList<String> Keys, String[] data, String[] KeySubsets, int first,
                                               int last, int currentIndex, int sizeComb) {

        // Current combination is ready to be printed, print it
        if (currentIndex == sizeComb) {
            for (int j = 0; j < sizeComb; j++) {
                KeySubsets[j] = KeySubsets[j];
            }
            return data;
        }

        // replace index with all possible elements. The condition
        // "end-i+1 >= r-index" makes sure that including one element
        // at index will make a combination with remaining elements
        // at remaining positions
        for (int i = first; i <= last && last - i + 1 >= sizeComb - first; i++) {
            KeySubsets[currentIndex] = Keys.get(i);
            GenerateKeysSubSets(Keys, data, KeySubsets, i + 1, last, currentIndex + 1, sizeComb);
        }
        return KeySubsets;
    }

    public static Comparator<String> x = new Comparator<String>()
    {
        @Override
        public int compare(String o1, String o2)
        {
            if(o1.length() > o2.length())
                return 1;

            if(o2.length() > o1.length())
                return -1;

            return 0;
        }
    };

}
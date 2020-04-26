package ch.ehi.oereb.webservice;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

import javax.annotation.PostConstruct;
import javax.xml.bind.JAXBContext;
import javax.xml.parsers.DocumentBuilderFactory;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.oxm.jaxb.Jaxb2Marshaller;
import org.springframework.test.context.junit4.SpringRunner;
import org.w3c.dom.Document;
import org.xmlunit.builder.DiffBuilder;
import org.xmlunit.diff.Diff;
import org.xmlunit.placeholder.PlaceholderDifferenceEvaluator;

import ch.ehi.ili2db.base.Ili2db;
import ch.ehi.ili2db.base.Ili2dbException;
import ch.ehi.ili2db.gui.Config;
import ch.ehi.ili2pg.PgMain;
import ch.ehi.oereb.schemas.oereb._1_0.extract.GetExtractByIdResponse;

// -Ddburl=jdbc:postgresql:dbname -Ddbusr=user -Ddbpwd=userpwd
@RunWith(SpringRunner.class)
@SpringBootTest
public class GetExtractTest {
    private static final String TEST_ILI = "src/test/ili";
    private static final String TEST_OUT = "build/ili2db";
    private static final String MODEL_DIR=Ili2db.ILI_FROM_DB+ch.interlis.ili2c.Main.ILIDIR_SEPARATOR+TEST_ILI; // +ch.interlis.ili2c.Main.ILIDIR_SEPARATOR+ch.interlis.ili2c.Main.ILI_REPOSITORY); 

    @Autowired
    OerebController service;
        
    @Autowired
    Jaxb2Marshaller marshaller;

    @Autowired
    JdbcTemplate jdbcTemplate;
    
    @Value("${oereb.dbschema}")
    private String DBSCHEMA;
    
    
    @PostConstruct
    public void setup() throws Exception
    {
        new File(TEST_OUT).mkdirs();
        Connection connection = null;
        try {
            connection=jdbcTemplate.getDataSource().getConnection();
            connection.setAutoCommit(false);
            jdbcTemplate.execute("DROP SCHEMA IF EXISTS "+DBSCHEMA+" CASCADE");
            {        
                Config config=new Config();
                new PgMain().initConfig(config);
                config.setJdbcConnection(connection);
                config.setDbschema(DBSCHEMA);
                config.setLogfile(new File(TEST_OUT,"ili1-import.log").getPath());
                config.setFunction(Config.FC_SCHEMAIMPORT);
                // --strokeArcs --createFk --createFkIdx --createGeomIdx   --createTidCol --createBasketCol --createImportTabs --createMetaInfo 
                // --disableNameOptimization --defaultSrsCode 2056
                // --models DM01AVCH24LV95D;PLZOCH1LV95D
                config.setStrokeArcs(Config.STROKE_ARCS_ENABLE);
                config.setCreateFk(Config.CREATE_FK_YES);
                config.setCreateFkIdx(Config.CREATE_FKIDX_YES);
                config.setValue(Config.CREATE_GEOM_INDEX,Config.TRUE);
                config.setTidHandling(Config.TID_HANDLING_PROPERTY);
                config.setBasketHandling(Config.BASKET_HANDLING_READWRITE);
                config.setCreateImportTabs(true);
                config.setCreateMetaInfo(true);
                config.setNameOptimization(Config.NAME_OPTIMIZATION_DISABLE);
                config.setDefaultSrsAuthority("EPSG");
                config.setDefaultSrsCode("2056");
                config.setModels("DM01AVCH24LV95D;PLZOCH1LV95D");
                config.setModeldir(MODEL_DIR); 
                Ili2db.readSettingsFromDb(config);
                Ili2db.run(config,null);
                connection.commit();
            }
            {        
                Config config=new Config();
                new PgMain().initConfig(config);
                config.setJdbcConnection(connection);
                config.setDbschema(DBSCHEMA);
                config.setLogfile(new File(TEST_OUT,"ili2-import.log").getPath());
                config.setFunction(Config.FC_SCHEMAIMPORT);
                // --models OeREBKRM_V1_1;OeREBKRMtrsfr_V1_1;OeREBKRMvs_V1_1;OeREB_ExtractAnnex_V1_0;SO_AGI_AV_GB_Administrative_Einteilungen_Publikation_20180822
                config.setModels("OeREBKRM_V1_1;OeREBKRMtrsfr_V1_1;OeREBKRMvs_V1_1;OeREB_ExtractAnnex_V1_0;SO_AGI_AV_GB_Administrative_Einteilungen_Publikation_20180822");
                config.setModeldir(MODEL_DIR); 
                Ili2db.readSettingsFromDb(config);
                Ili2db.run(config,null);
                connection.commit();
            }
            {
                // AV-Daten
                File data=new File("src/test/data/av_test.itf");
                importFile(data);
            }
            {
                // OEREB RM Codelisten
                File data=new File("src/test/data/OeREBKRM_V1_1_Codelisten_20170101.xml");
                importFile(data);
            }
            {
                // Gesetze Bund
                File data=new File("src/test/data/OeREBKRM_V1_1_Gesetze_20180501.xml");
                importFile(data);
            }
            {
                // GB-Kreise
                File data=new File("src/test/data/administrative-einteilung.xtf");
                importFile(data);
            }
            {
                File data=new File("src/test/data/annex-AktiveGemeinden.xtf");
                importFile(data);
            }
            {
                File data=new File("src/test/data/annex-glossar.xtf");
                importFile(data);
            }
            {
                File data=new File("src/test/data/annex-KatatasterAmt.xtf");
                importFile(data);
            }
            {
                File data=new File("src/test/data/annex-logos.xtf");
                importFile(data);
            }
            {
                File data=new File("src/test/data/annex-logos-2498.xtf");
                importFile(data);
            }
            {
                File data=new File("src/test/data/annex-logos-2500.xtf");
                importFile(data);
            }
            {
                File data=new File("src/test/data/annex-logos-2502.xtf");
                importFile(data);
            }
            {
                File data=new File("src/test/data/annex-texte.xtf");
                importFile(data);
            }
            {
                File data=new File("src/test/data/annex-themen.xtf");
                importFile(data);
            }
            {
                // OEREB Transfer
                File data=new File("src/test/data/oereb_test.xtf");
                importFile(data);
            }
        }finally {
            if(connection!=null) {
                connection.close();
                connection=null;
            }
        }
    }
    public void importFile(File data) throws Exception {
        
        Connection connection = null;
        try {
            Config config=new Config();
            new PgMain().initConfig(config);
            connection = jdbcTemplate.getDataSource().getConnection();
            connection.setAutoCommit(false);
            config.setJdbcConnection(connection);
            config.setDbschema(DBSCHEMA);
            config.setLogfile(new File(TEST_OUT,data.getName()+"-import.log").getPath());
            config.setXtffile(data.getPath());
            if(Ili2db.isItfFilename(data.getPath())){
                config.setItfTransferfile(true);
            }
            config.setFunction(Config.FC_IMPORT);
            config.setDatasetName(ch.ehi.basics.view.GenericFileFilter.stripFileExtension(data.getName()));
            config.setImportTid(true);
            config.setModeldir(MODEL_DIR); 
            Ili2db.readSettingsFromDb(config);
            Ili2db.run(config,null);
            connection.commit();
        }finally {
            if(connection!=null) {
                connection.close();
                connection=null;
            }
        }
    }
    public static org.xmlunit.matchers.CompareMatcher createMatcher(File controlFile) {
        return org.xmlunit.matchers.CompareMatcher.isSimilarTo(controlFile).ignoreWhitespace().ignoreComments();
        /*
            You can now try ${xmlunit.ignore} in XMLUnit 2.6.0 (add dependency xmlunit-placeholders). Sample code is as below.

            Diff diff = DiffBuilder
            .compare(expectedXML)
            .withTest(actualXML)
            .withDifferenceEvaluator(new PlaceholderDifferenceEvaluator())
            .build();
             */
    }
    //@Test
    // CH740632871570 Liegenschaft in Gemeinde (2500) ohne OEREB Themen
    //@Ignore("requires sql fixing")
    public void Liegenschaft_in_Gemeinde_ohne_OEREB_Themen() throws Exception 
    {
    }
    // CH580632068782 SDR mit OEREBs (P,L,F) plus eine angeschnitten
    @Test
    public void SDR() throws Exception 
    {
        Assert.assertNotNull(service);
        ResponseEntity<GetExtractByIdResponse> response = (ResponseEntity<GetExtractByIdResponse>) service.getExtractWithGeometryByEgrid("xml","CH580632068782",null,null,null);
        marshaller.marshal(response.getBody(),new javax.xml.transform.stream.StreamResult("tmp/out.xml"));
        File controlFile = new File("src/test/data-expected/CH580632068782.xml");
        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        Document doc = dbf.newDocumentBuilder().newDocument(); 
        marshaller.marshal(response.getBody(), new javax.xml.transform.dom.DOMResult(doc));
        //Assert.assertThat(doc,createMatcher(controlFile));
        Diff diff = DiffBuilder
        .compare(controlFile)
        .withTest(doc)
        .withDifferenceEvaluator(new PlaceholderDifferenceEvaluator())
        .ignoreComments()
        .ignoreWhitespace()
        .checkForSimilar()
        .build();
        //System.out.println(diff.toString());
        Assert.assertFalse(diff.hasDifferences());
    }

    // CH133289063542 Liegenschaft ohne OEREBs, keine anderen OEREBs im sichtbaren Bereich
    @Test
    public void Liegenschaft_ohneOEREBs() throws Exception 
    {
        Assert.assertNotNull(service);
        ResponseEntity<GetExtractByIdResponse> response = (ResponseEntity<GetExtractByIdResponse>) service.getExtractWithGeometryByEgrid("xml","CH133289063542",null,null,null);
        marshaller.marshal(response.getBody(),new javax.xml.transform.stream.StreamResult("tmp/out.xml"));
        File controlFile = new File("src/test/data-expected/CH133289063542.xml");
        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        Document doc = dbf.newDocumentBuilder().newDocument(); 
        marshaller.marshal(response.getBody(), new javax.xml.transform.dom.DOMResult(doc));
        //Assert.assertThat(doc,createMatcher(controlFile));
        Diff diff = DiffBuilder
        .compare(controlFile)
        .withTest(doc)
        .withDifferenceEvaluator(new PlaceholderDifferenceEvaluator())
        .ignoreComments()
        .ignoreWhitespace()
        .checkForSimilar()
        .build();
        //System.out.println(diff.toString());
        Assert.assertFalse(diff.hasDifferences());
    }
    // CH793281100623 Liegenschaft ohne OEREBs, aber alle OEREBs von im sichtbaren Bereich (otherLegends)
    @Test
    public void Liegenschaft_otherLegends() throws Exception 
    {
        Assert.assertNotNull(service);
        ResponseEntity<GetExtractByIdResponse> response = (ResponseEntity<GetExtractByIdResponse>) service.getExtractWithGeometryByEgrid("xml","CH793281100623",null,null,null);
        marshaller.marshal(response.getBody(),new javax.xml.transform.stream.StreamResult("tmp/out.xml"));
        File controlFile = new File("src/test/data-expected/CH793281100623.xml");
        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        Document doc = dbf.newDocumentBuilder().newDocument(); 
        marshaller.marshal(response.getBody(), new javax.xml.transform.dom.DOMResult(doc));
        //Assert.assertThat(doc,createMatcher(controlFile));
        Diff diff = DiffBuilder
        .compare(controlFile)
        .withTest(doc)
        .withDifferenceEvaluator(new PlaceholderDifferenceEvaluator())
        .ignoreComments()
        .ignoreWhitespace()
        .checkForSimilar()
        .build();
        //System.out.println(diff.toString());
        Assert.assertFalse(diff.hasDifferences());
    }
}

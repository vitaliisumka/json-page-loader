package com.pointcarbon.loaders.FLOW;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pointcarbon.esb.commons.beans.EsbMessage;
import com.pointcarbon.loaderframework.service.DownloadHelper;
import com.pointcarbon.loaderframework.service.ILoader;
import com.pointcarbon.loaderframework.service.LoaderRunService;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;

public class JSONPageLoader implements ILoader {
    private static final String PART_OF_API = "http://qte.int.test.com/repon/rest/groups/";
    private static final String HEADER_REQUEST_KEY = "header-requst-key-14952";
    private static final String HEADER_REQUEST_VALUE = "web.data.lineup";
    private static final String JSON_PATH_ID = "id";
    private static final String JSON_PATH_EPOCH_TIME = "originalPublishDate";
    private static final String JSON_PATH_FILE_DETAILS = "fileDetails";
    private static final String JSON_PATH_FILE_NAME = "name";
    private final DateTimeZone TIME_ZONE = DateTimeZone.forID("CET");
    private static final Logger log = LoggerFactory.getLogger(JSONPageLoader.class);

    @Override
    public void load(DateTime focusDate, DownloadHelper downloadHelper, EsbMessage message) throws Exception {
        int startDateOffset = Integer.parseInt(message.getParameterValue("startdate_offset"));
        int endDateOffset = Integer.parseInt(message.getParameterValue("enddate_offset"));

        DateTime startDate = new DateTime().plusDays(startDateOffset).withTimeAtStartOfDay();
        DateTime endDate = new DateTime().plusDays(endDateOffset).withTimeAtStartOfDay();

        String groupId = message.getParameterValue(LoaderRunService.PARAMETER);
        String api = createAPI(groupId);
        String json = getJSON(api);
        jsonIterator(json, api, startDate, endDate, downloadHelper);

    }

    private void jsonIterator(String json, String api, DateTime startDate, DateTime endDate, DownloadHelper downloadHelper) throws Exception {
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode jsonContent = objectMapper.readTree(json);
        Iterator<JsonNode> resourceIterator = jsonContent.elements();

        while (resourceIterator.hasNext()) {
            JsonNode jsonTree = resourceIterator.next();
            String id = jsonTree.get(JSON_PATH_ID).asText();
            String epochTime = jsonTree.get(JSON_PATH_EPOCH_TIME).asText();
            DateTime dateTime = new DateTime().withMillis(Long.parseLong(epochTime)).withZone(TIME_ZONE);

            if (!dateTime.isBefore(startDate) && !dateTime.isAfter(endDate)) {
                JsonNode fileDetails = jsonTree.path(JSON_PATH_FILE_DETAILS);
                String name = fileDetails.path(JSON_PATH_FILE_NAME).asText();
                String url = createUrlToFile(api, id);
                sendFile(url, downloadHelper, name);
            }
        }
    }

    private String createUrlToFile(String api, String id) {
        return api + id + "/file";
    }

    private String createAPI(String groupId) {
        return PART_OF_API + groupId + "/documents/";
    }

    private HttpGet httpGetSetup(String url) {
        final HttpGet httpGet = new HttpGet(url);
        httpGet.setHeader(HEADER_REQUEST_KEY, HEADER_REQUEST_VALUE);
        return httpGet;
    }

    private String getJSON(String url) throws IOException {
        try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
            final HttpGet httpGet = httpGetSetup(url);
            try (CloseableHttpResponse response = httpClient.execute(httpGet)) {
                return EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8);
            }
        }
    }

    private void sendFile(String url, DownloadHelper downloadHelper, String fileName) throws Exception {
        try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
            final HttpGet httpGet = httpGetSetup(url);
            try (CloseableHttpResponse response = httpClient.execute(httpGet)) {
                byte[] responseBody = EntityUtils.toByteArray(response.getEntity());
                downloadHelper.withModifiedContent(fileName, responseBody).send();
            }
        }
    }

}

package com.spotify.apollo.elide;

import static com.spotify.apollo.Status.CREATED;
import static com.spotify.apollo.Status.METHOD_NOT_ALLOWED;
import static com.spotify.apollo.Status.NOT_ACCEPTABLE;
import static com.spotify.apollo.Status.NO_CONTENT;
import static com.spotify.apollo.Status.OK;
import static com.spotify.apollo.Status.UNSUPPORTED_MEDIA_TYPE;
import static com.spotify.apollo.test.unit.ResponseMatchers.hasHeader;
import static com.spotify.apollo.test.unit.ResponseMatchers.hasStatus;
import static com.spotify.apollo.test.unit.StatusTypeMatchers.withCode;
import static java.util.Optional.empty;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.CoreMatchers.any;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.spotify.apollo.Client;
import com.spotify.apollo.Request;
import com.spotify.apollo.Response;
import com.spotify.apollo.Status;
import com.spotify.apollo.elide.testmodel.Thing;
import com.spotify.apollo.request.RequestContexts;
import com.spotify.apollo.request.RequestMetadataImpl;
import com.spotify.apollo.route.AsyncHandler;
import com.spotify.apollo.route.Route;
import com.spotify.apollo.test.StubClient;
import com.yahoo.elide.Elide;
import com.yahoo.elide.core.DataStoreTransaction;
import com.yahoo.elide.datastores.inmemory.InMemoryDataStore;
import java.io.IOException;
import java.time.Instant;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import okio.ByteString;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class ElideResourceTest {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final String PREFIX = "/prefix";

  private ElideResource resource;

  private Elide elide;
  private InMemoryDataStore dataStore;

  private Client client = new StubClient();

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Before
  public void setUp() throws Exception {
    dataStore = new InMemoryDataStore(Package.getPackage("com.spotify.apollo.elide.testmodel"));
    elide = new Elide.Builder(dataStore).build();

    resource = new ElideResource(elide,
        PREFIX,
        rc -> null,
        EnumSet.allOf(ElideResource.Methods.class));

    addToDataStore(new Thing("1", "flerp"));
    addToDataStore(new Thing("2", "florpe"));
  }

  @Test
  public void shouldReturnSingleElementFromElide() throws Exception {
    Response<ByteString> response = invokeRoute(Request.forUri("/prefix/thing/1", "GET"));

    JsonNode jsonNode = successfulAsJson(response);

    assertThat(jsonNode.get("data").get("id").asText(), is("1"));
    assertThat(jsonNode.get("data").get("attributes").get("name").asText(), is("flerp"));
  }

  @Test
  public void shouldReturnCollectionFromElide() throws Exception {

    Response<ByteString> response = invokeRoute(Request.forUri("/prefix/thing", "GET"));

    JsonNode jsonNode = successfulAsJson(response);

    Set<String> ids = new HashSet<>();
    Set<String> names = new HashSet<>();

    JsonNode data = jsonNode.get("data");

    for (int i = 0; i < data.size(); i++) {
      ids.add(data.get(i).get("id").asText());
      names.add(data.get(i).get("attributes").get("name").asText());
    }

    assertThat(ids, is(ImmutableSet.of("1", "2")));
    assertThat(names, is(ImmutableSet.of("flerp", "florpe")));
  }

  @Test
  public void shouldReturn404ForUnknownCollection() throws Exception {
    Response<ByteString> response = invokeRoute(Request.forUri("/prefix/nonexistent", "GET"));

    assertThat(response, hasStatus(withCode(Status.NOT_FOUND)));
  }

  @Test
  public void shouldSupportSparseFieldsInGet() throws Exception {
    JsonNode jsonNode = successfulAsJson(
        invokeRoute(Request.forUri("/prefix/thing/1?fields[thing]=description", "GET")));

    assertThat(jsonNode.get("data").get("attributes").has("description"), is(true));
    assertThat(jsonNode.get("data").get("attributes").has("name"), is(false));
  }

  @Test
  public void shouldReturn405IfMethodNotEnabled() throws Exception {
    resource = new ElideResource(elide, PREFIX, rc -> null, EnumSet.of(ElideResource.Methods.POST));

    Response<ByteString> response = invokeRoute(Request.forUri("/prefix/thing", "GET"));

    assertThat(response, hasStatus(withCode(Status.METHOD_NOT_ALLOWED)));
  }

  @Test
  public void shouldSetContentHeader() throws Exception {
    Response<ByteString> response = invokeRoute(Request.forUri("/prefix/thing", "GET"));

    assertThat(response, hasHeader("content-type", equalTo("application/vnd.api+json")));
  }

  @Test
  public void shouldSupportPrefixWithTrailingSlash() throws Exception {
    resource = new ElideResource(elide, PREFIX + "/", rc -> null, EnumSet.of(ElideResource.Methods.GET));

    Response<ByteString> response = invokeRoute(Request.forUri("/prefix/thing", "GET"));

    assertThat(response, hasStatus(withCode(Status.OK)));
  }

  @Test
  public void shouldFailForPrefixWithoutLeadingSlash() throws Exception {
    thrown.expect(IllegalArgumentException.class);

    new ElideResource(elide, PREFIX.substring(1), rc -> null, EnumSet.of(ElideResource.Methods.GET));
  }

  @Test
  public void shouldSupportPost() throws Exception {
    Response<ByteString> response = invokeRoute(Request.forUri("/prefix/thing", "POST")
        .withPayload(toBody(new Thing("3", "posted"))));

    if (response.payload().isPresent()) {
      assertThat(response, hasStatus(withCode(CREATED)));
    } else {
      assertThat(response, hasStatus(withCode(NO_CONTENT)));
    }
  }

  @Test
  public void shouldReturn201ForCreateWithoutId() throws Exception {
    Thing thing = new Thing("1", "hasNoId");
    thing.id = null;

    Response<ByteString> response = invokeRoute(Request.forUri("/prefix/thing", "POST")
        .withPayload(toBody(thing)));

    JsonNode jsonNode = bodyWithExpectedStatus(response, CREATED);

    assertThat(jsonNode.get("data").get("attributes").get("name").asText(), is("hasNoId"));
  }

  @Test
  public void shouldIncludeLocationHeaderForCreateWithoutId() throws Exception {
    Thing thing = new Thing("1", "hasNoId");
    thing.id = null;

    Response<ByteString> response = invokeRoute(Request.forUri("/prefix/thing", "POST")
        .withPayload(toBody(thing)));

    assertThat(response, hasHeader("location", any(String.class)));
  }

  @Test
  public void shouldSupportDelete() throws Exception {
    Response<ByteString> response = invokeRoute(Request.forUri("/prefix/thing/1", "DELETE"));

    assertThat(response, hasStatus(withCode(NO_CONTENT)));
  }

  @Test
  public void shouldSupportPatch() throws Exception {
    Thing thing = new Thing("1", null);
    thing.description = "cooldesc";

    Response<ByteString> response = invokeRoute(Request.forUri("/prefix/thing/1", "PATCH")
        .withPayload(toBody(thing)));

    JsonNode jsonNode = successfulAsJson(response);

    assertThat(jsonNode.get("data").get("attributes").get("description"), is("cooldesc"));
    assertThat(jsonNode.get("data").get("attributes").get("name"), is("flerp"));
  }

  @Test
  public void shouldReturn405ForPut() throws Exception {
    Response<ByteString> response = invokeRoute(Request.forUri("/prefix/thing/1", "PUT")
        .withPayload(toBody(new Thing("19", "hi"))));

    assertThat(response, hasStatus(withCode(METHOD_NOT_ALLOWED)));
  }

  @Test
  public void shouldReturn415ForMediaTypeParametersInRequestContentType() throws Exception {
    Response<ByteString> response = invokeRoute(Request.forUri("/prefix/thing", "POST")
        .withHeader("content-type", "application/vnd.api+json; charset=utf-8")
        .withPayload(toBody(new Thing("19", "hi"))));

    assertThat(response, hasStatus(withCode(UNSUPPORTED_MEDIA_TYPE)));
  }

  @Test
  public void shouldReturn406ForMediaTypeParametersInAllAcceptOptions() throws Exception {
    Response<ByteString> response = invokeRoute(Request.forUri("/prefix/thing", "GET")
        .withHeader("Accept",
            "application/vnd.api+json;charset=utf-8, application/vnd.api+json;charset=us-ascii")
        .withPayload(toBody(new Thing("19", "hi"))));

    assertThat(response, hasStatus(withCode(NOT_ACCEPTABLE)));
  }

  @Test
  public void shouldSupportMediaTypeParametersInOneAcceptOptions() throws Exception {
    Response<ByteString> response = invokeRoute(Request.forUri("/prefix/thing", "GET")
        .withHeader("Accept", "application/vnd.api+json;charset=utf-8,application/vnd.api+json")
        .withPayload(toBody(new Thing("19", "hi"))));

    assertThat(response, hasStatus(withCode(OK)));
  }

  @Test
  public void shouldPassUserSuppliedByFunctionToElide() throws Exception {
    fail();
  }

  private void addToDataStore(Thing thing) {
    DataStoreTransaction transaction = dataStore.beginTransaction();
    transaction.preCommit();
    transaction.save(thing);
    transaction.commit();
  }

  private Response<ByteString> invokeRoute(Request request) throws Exception {
    List<Route<AsyncHandler<Response<ByteString>>>> routes = resource.routes()
        .filter(r -> r.method().equals(request.method()))
        .collect(toList());

    assertThat(routes.size(), is(1));

    return routes.get(0).handler().invoke(
        RequestContexts.create(
            request,
            client,
            pathArgs(request.uri()),
            0,
            RequestMetadataImpl.create(Instant.now(), empty(), empty())))
        .toCompletableFuture().get();
  }

  private Map<String, String> pathArgs(String uri) {
    int queryStartIndex = uri.indexOf('?');

    String queryPath = queryStartIndex < 0 ?
                       uri.substring(PREFIX.length() + 1) :
                       uri.substring(PREFIX.length() + 1, queryStartIndex);
    return ImmutableMap.of("query-path", queryPath);
  }

  private JsonNode successfulAsJson(Response<ByteString> response) throws Exception {
    return bodyWithExpectedStatus(response, OK);
  }

  private JsonNode bodyWithExpectedStatus(Response<ByteString> response, Status status) throws IOException {
    assertThat(response, hasStatus(withCode(status)));
    assertThat(response.payload().isPresent(), is(true));

    //noinspection OptionalGetWithoutIsPresent - checked above
    return OBJECT_MAPPER.readTree(response.payload().get().utf8());
  }

  private ByteString toBody(Thing thing) {
    return ByteString.encodeUtf8(
        String.format("{ \"data\": {"
                      + "\"id\": \"%s\","
                      + "\"type\": \"thing\","
                      + "\"attributes\": {"
                      + "\"name\": \"%s\", "
                      + "\"description\": \"%s\""
                      + "}"
                      + "}"
                      + "}", thing.id, thing.name, thing.description)
    );
  }
}
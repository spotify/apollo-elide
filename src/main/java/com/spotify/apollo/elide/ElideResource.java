package com.spotify.apollo.elide;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

import com.google.common.base.Splitter;
import com.google.common.net.MediaType;
import com.spotify.apollo.Request;
import com.spotify.apollo.RequestContext;
import com.spotify.apollo.Response;
import com.spotify.apollo.Status;
import com.spotify.apollo.StatusType;
import com.spotify.apollo.route.AsyncHandler;
import com.spotify.apollo.route.Route;
import com.yahoo.elide.Elide;
import com.yahoo.elide.ElideResponse;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Stream;
import javax.ws.rs.core.MultivaluedHashMap;
import okio.ByteString;

/**
 * Hooks up Apollo endpoints with Elide.
 */
public class ElideResource {
  public enum Methods {
    GET,
    POST,
    PATCH,
    DELETE
  }

  private final Elide elide;
  private final String pathPrefix;
  private final Function<RequestContext, Object> userFunction;

  ElideResource(Elide elide,
                String pathPrefix,
                Function<RequestContext, Object> userFunction,
                Set<Methods> enabledMethods) {
    checkArgument(pathPrefix.startsWith("/"), "Path prefix must start with '/' (got '%s')", pathPrefix);
    this.elide = requireNonNull(elide);
    this.pathPrefix = pathPrefix.endsWith("/") ? pathPrefix : pathPrefix + "/";
    this.userFunction = requireNonNull(userFunction);
  }

  Stream<Route<AsyncHandler<Response<ByteString>>>> routes() {
    return Stream.of(
        Route.sync("GET", pathPrefix + "<query-path:path>", this::get),
        Route.sync("POST", pathPrefix + "<query-path:path>", this::post),
        Route.sync("DELETE", pathPrefix + "<query-path:path>", this::delete),
        Route.sync("PUT", pathPrefix + "<query-path:path>", this::put),
        Route.sync("PATCH", pathPrefix + "<query-path:path>", this::patch)
    )
        .map(r -> r.withMiddleware(this::serializeJsonApi))
        .map(r -> r.withMiddleware(ElideResource::validateHeaders));
  }

  private static AsyncHandler<Response<ByteString>> validateHeaders(
      AsyncHandler<Response<ByteString>> handler) {
    return requestContext -> {
      if (contentTypeHasMediaTypeParameters(requestContext.request())) {
        return CompletableFuture
            .completedFuture(Response.<ByteString>forStatus(Status.UNSUPPORTED_MEDIA_TYPE));
      } else if (acceptHeaderHasNoParameterlessJsonApiMediaType(requestContext.request())) {
        return CompletableFuture
            .completedFuture(Response.<ByteString>forStatus(Status.NOT_ACCEPTABLE));
      } else {
        return handler.invoke(requestContext);
      }
    };
  }

  private static boolean acceptHeaderHasNoParameterlessJsonApiMediaType(Request request) {
    Optional<String> header = headerValueIgnoreCase(request, "accept");

    if (!header.isPresent()) {
      return false;
    }

    Optional<String> mediaTypeWithoutParameter = Splitter.on(',').trimResults()
        .splitToList(header.get()).stream()
        .filter(s -> MediaType.parse(s).parameters().isEmpty())
        .findAny();

    return !mediaTypeWithoutParameter.isPresent();
  }

  private static boolean contentTypeHasMediaTypeParameters(Request request) {
    Optional<String> header = headerValueIgnoreCase(request, "content-type");

    return header.isPresent() && !MediaType.parse(header.get()).parameters().isEmpty();
  }

  private static Optional<String> headerValueIgnoreCase(Request request, String headerKey) {
    return request.headers().entrySet().stream()
          .filter(entry -> entry.getKey().equalsIgnoreCase(headerKey))
          .map(Map.Entry::getValue)
          .findAny();
  }

  private AsyncHandler<Response<ByteString>> serializeJsonApi(
      AsyncHandler<Response<String>> handler) {
    return requestContext -> handler.invoke(requestContext)
        .thenApply(response -> response.withHeader("content-type", "application/vnd.api+json")
            .withPayload(response.payload().map(ByteString::encodeUtf8).orElse(null)));
  }

  private Response<String> get(RequestContext requestContext) {
    ElideResponse response =
        // TODO: do something better wrt the user; should standardise on something; this something
        // should probably be oauth-related somehow
        elide.get(requestContext.pathArgs().get("query-path"),
            queryParams(requestContext.request().parameters()),
            null);

    return Response.of(Status.createForCode(response.getResponseCode()),  response.getBody());
  }

  private Response<String> post(RequestContext requestContext) {
    String body = payloadAsString(requestContext);

    ElideResponse response =
        // TODO: do something better wrt the user; should standardise on something; this something
        // should probably be oauth-related somehow
        elide.post(requestContext.pathArgs().get("query-path"), body, null);

    return toApolloResponse(response);
  }

  private Response<String> delete(RequestContext requestContext) {
    ElideResponse response =
        // TODO: do something better wrt the user; should standardise on something; this something
        // should probably be oauth-related somehow
        elide.delete(
            requestContext.pathArgs().get("query-path"),
            payloadAsString(requestContext),
            null);

    return toApolloResponse(response);
  }

  private Response<String> put(RequestContext requestContext) {
    return Response.forStatus(Status.INTERNAL_SERVER_ERROR);
  }

  private Response<String> patch(RequestContext requestContext) {
    Request request = requestContext.request();

    ElideResponse response =
        // TODO: do something better wrt the user; should standardise on something; this something
        // should probably be oauth-related somehow
        elide.patch(
            headerValueIgnoreCase(request, "content-type").orElse(null),
            headerValueIgnoreCase(request, "accept").orElse(null),
            requestContext.pathArgs().get("query-path"),
            payloadAsString(requestContext),
            null);

    return toApolloResponse(response);
  }

  private Response<String> toApolloResponse(ElideResponse response) {
    StatusType statusCode = Status.createForCode(response.getResponseCode());

    if (response.getBody() == null) {
      return Response.forStatus(statusCode);
    }

    return Response.of(statusCode, response.getBody());
  }

  private String payloadAsString(RequestContext requestContext) {
    return requestContext.request().payload()
        .map(ByteString::utf8)
        .orElse(null);
  }

  private MultivaluedHashMap<String, String> queryParams(Map<String, List<String>> parameters) {
    MultivaluedHashMap<String, String> map = new MultivaluedHashMap<>();

    for (String queryParameterName : parameters.keySet()) {
      map.put(queryParameterName, parameters.get(queryParameterName));
    }

    return map;
  }
}

/*
 * -\-\-
 * Apollo Elide integration
 * --
 * Copyright (C) 2016 Spotify AB
 * --
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * -/-/-
 */
package com.spotify.apollo.elide;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableSet;
import com.google.common.net.MediaType;
import com.spotify.apollo.Request;
import com.spotify.apollo.RequestContext;
import com.spotify.apollo.Response;
import com.spotify.apollo.Status;
import com.spotify.apollo.StatusType;
import com.spotify.apollo.route.AsyncHandler;
import com.spotify.apollo.route.Route;
import com.spotify.apollo.route.SyncHandler;
import com.yahoo.elide.Elide;
import com.yahoo.elide.ElideResponse;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Stream;
import javax.ws.rs.core.MultivaluedHashMap;
import okio.ByteString;

/**
 * Hooks up Apollo endpoints with Elide. Instantiate via the {@link #builder(String, Elide)} method.
 */
public class ElideResource {

  private static final String PATH_PARAMETER_NAME = "query-path";

  public enum Method {
    GET(ElideResource::get),
    POST(ElideResource::post),
    PATCH(ElideResource::patch),
    DELETE(ElideResource::delete);

    private final BiFunction<Elide, Function<RequestContext, Object>, SyncHandler<Response<String>>> handler;

    Method(BiFunction<Elide, Function<RequestContext, Object>, SyncHandler<Response<String>>> handler) {
      this.handler = handler;
    }
  }

  private final Elide elide;
  private final String pathPrefix;
  private final Function<RequestContext, Object> userFunction;
  private final Set<Method> enabledMethods;

  private ElideResource(Elide elide,
                String pathPrefix,
                Function<RequestContext, Object> userFunction,
                Set<Method> enabledMethods) {
    this.elide = requireNonNull(elide);
    this.pathPrefix = pathPrefix.endsWith("/") ? pathPrefix : pathPrefix + "/";
    this.userFunction = requireNonNull(userFunction);
    this.enabledMethods = ImmutableSet.copyOf(enabledMethods);
  }

  public Stream<Route<AsyncHandler<Response<ByteString>>>> routes() {
    return enabledMethods.stream()
        .map(method -> Route.sync(
            method.name(),
            pathPrefix + "<query-path:path>",
            method.handler.apply(elide, userFunction)))
        .map(r -> r.withMiddleware(this::serializeJsonApi))
        .map(r -> r.withMiddleware(ElideResource::validateHeaders));
  }

  private static AsyncHandler<Response<ByteString>> validateHeaders(
      AsyncHandler<Response<ByteString>> handler) {
    return requestContext -> {
      if (contentTypeHasMediaTypeParameters(requestContext.request())) {
        return completedFuture(Response.<ByteString>forStatus(Status.UNSUPPORTED_MEDIA_TYPE));
      } else if (acceptHeaderHasNoJsonApiMediaTypeWithoutParameters(requestContext.request())) {
        return completedFuture(Response.<ByteString>forStatus(Status.NOT_ACCEPTABLE));
      } else {
        return handler.invoke(requestContext);
      }
    };
  }

  private static boolean acceptHeaderHasNoJsonApiMediaTypeWithoutParameters(Request request) {
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

  private static SyncHandler<Response<String>> get(
      Elide elide,
      Function<RequestContext, Object> userFunction) {
    return requestContext -> toApolloResponse(
        elide.get(
            requestContext.pathArgs().get(PATH_PARAMETER_NAME),
            queryParams(requestContext.request().parameters()),
            userFunction.apply(requestContext)));
  }

  private static SyncHandler<Response<String>> post(Elide elide,
                                                    Function<RequestContext, Object> userFunction) {
    return requestContext -> {
      if (isPatchOverride(requestContext.request())) {
        return doPatch(requestContext, elide, userFunction);
      }

      String body = payloadAsString(requestContext);

      return toApolloResponse(
          elide.post(
              requestContext.pathArgs().get(PATH_PARAMETER_NAME),
              body,
              userFunction.apply(requestContext)));
    };
  }

  private static boolean isPatchOverride(Request request) {
    return request.header("X-HTTP-Method-Override")
        .map(override -> override.equalsIgnoreCase("PATCH"))
        .orElse(false);
  }

  private static SyncHandler<Response<String>> delete(Elide elide,
                                                      Function<RequestContext, Object> userFunction) {

    return requestContext -> toApolloResponse(
        elide.delete(
            requestContext.pathArgs().get(PATH_PARAMETER_NAME),
            payloadAsString(requestContext),
            userFunction.apply(requestContext)));
  }

  private static SyncHandler<Response<String>> patch(Elide elide,
                                                     Function<RequestContext, Object> userFunction) {
    return requestContext -> doPatch(requestContext, elide, userFunction);
  }

  private static Response<String> doPatch(RequestContext requestContext,
                                          Elide elide,
                                          Function<RequestContext, Object> userFunction) {
    Request request = requestContext.request();

    return toApolloResponse(
        elide.patch(
            headerValueIgnoreCase(request, "content-type").orElse(null),
            headerValueIgnoreCase(request, "accept").orElse(null),
            requestContext.pathArgs().get(PATH_PARAMETER_NAME),
            payloadAsString(requestContext),
            userFunction.apply(requestContext)));
  }

  private static Response<String> toApolloResponse(ElideResponse response) {
    StatusType statusCode = Status.createForCode(response.getResponseCode());

    if (response.getBody() == null) {
      return Response.forStatus(statusCode);
    }

    return Response.of(statusCode, response.getBody());
  }

  private static String payloadAsString(RequestContext requestContext) {
    return requestContext.request().payload()
        .map(ByteString::utf8)
        .orElse(null);
  }

  private static MultivaluedHashMap<String, String> queryParams(Map<String, List<String>> parameters) {
    MultivaluedHashMap<String, String> map = new MultivaluedHashMap<>();

    for (String queryParameterName : parameters.keySet()) {
      map.put(queryParameterName, parameters.get(queryParameterName));
    }

    return map;
  }

  /**
   * Create a builder for a resource that will pick up requests starting with the supplied path
   * prefix.
   *
   * @throws IllegalArgumentException unless the path starts with a forward slash
   */
  public static Builder builder(String pathPrefix, Elide elide) {
    return new Builder(pathPrefix, elide);
  }

  public static class Builder {
    private final String pathPrefix;
    private final Elide elide;
    private Function<RequestContext, Object> userFunction = rc -> null;
    private Set<Method> enabledMethods = EnumSet.allOf(Method.class);

    private Builder(String pathPrefix, Elide elide) {
      checkArgument(
          pathPrefix.startsWith("/"),
          "Path prefix must start with '/' (got '%s')", pathPrefix);
      this.pathPrefix = requireNonNull(pathPrefix);
      this.elide = requireNonNull(elide);
    }

    /**
     * Configure the function that creates a 'user' object for use with Elide access control based
     * on the {@link RequestContext}. See http://elide.io/pages/guide/03-security.html for details
     * about security in Elide.
     * <p/>
     * Defaults to a function that always returns 'null'.
     */
    public Builder userFunction(Function<RequestContext, Object> userFunction) {
      this.userFunction = userFunction;
      return this;
    }

    /**
     * Determine which {@link Method}s should be enabled for this resource.
     * <p/>
     * Defaults to 'all methods enabled'.
     */
    public Builder enabledMethods(Set<Method> enabledMethods) {
      this.enabledMethods = ImmutableSet.copyOf(enabledMethods);
      return this;
    }

    public ElideResource build() {
      return new ElideResource(elide, pathPrefix, userFunction, enabledMethods);
    }
  }
}

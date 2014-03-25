package edu.jhu.pha.vospace.oauth;

import java.util.Map;

import javax.annotation.Priority;
import javax.ws.rs.core.Configuration;
import javax.ws.rs.core.FeatureContext;

import org.glassfish.jersey.internal.util.PropertiesHelper;
import org.glassfish.jersey.oauth1.signature.OAuth1SignatureFeature;
import org.glassfish.jersey.server.model.ModelProcessor;
import org.glassfish.jersey.server.model.Resource;
import org.glassfish.jersey.server.model.ResourceModel;
import org.glassfish.jersey.server.oauth1.OAuth1Provider;
import org.glassfish.jersey.server.oauth1.OAuth1ServerProperties;
import org.glassfish.jersey.server.oauth1.internal.AccessTokenResource;
import org.glassfish.jersey.server.oauth1.internal.RequestTokenResource;

/**
 * Copy of OAUth1ServerFeature, to use my own DB Nonce manager for security
 * @author dimm
 *
 */
public class MyOAuthServerFeature implements javax.ws.rs.core.Feature {
    private final OAuth1Provider oAuth1Provider;
    private final String requestTokenUri;
    private final String accessTokenUri;

    /**
     * Create a new feature configured with {@link OAuth1Provider OAuth provider} and request and access token
     * URIs. The feature also exposes Request and Access Token Resources.
     * These resources are part of the Authorization process and
     * grant Request and Access tokens. Resources will be available on
     * URIs defined by parameters {@code requestTokenUri} and {@code accessTokenUri}.
     *
     * @param oAuth1Provider Instance of the {@code OAuth1Provider} that will handle authorization. If the value is
     *                       {@code null}, then the provider must be registered explicitly outside of this feature
     *                       as a standard provider.
     * @param requestTokenUri URI (relative to application context path) of Request Token Resource that will be exposed.
     * @param accessTokenUri URI (relative to application context path) of Request Token Resource that will be exposed.
     */
    public MyOAuthServerFeature(OAuth1Provider oAuth1Provider,
                               String requestTokenUri,
                               String accessTokenUri) {
        this.oAuth1Provider = oAuth1Provider;
        this.requestTokenUri = requestTokenUri;
        this.accessTokenUri = accessTokenUri;
    }
	
	@Override
    public boolean configure(FeatureContext context) {
        if (oAuth1Provider != null) {
            context.register(oAuth1Provider);
        } else {
        	System.err.println("!!!!! NULL");
        }

        context.register(MyOAuthServerFilter.class);

        if (!context.getConfiguration().isRegistered(OAuth1SignatureFeature.class)) {
            context.register(OAuth1SignatureFeature.class);
        }

        final Map<String, Object> properties = context.getConfiguration().getProperties();
        final Boolean propertyResourceEnabled = PropertiesHelper.getValue(properties,
                OAuth1ServerProperties.ENABLE_TOKEN_RESOURCES, null, Boolean.class);

        boolean registerResources = propertyResourceEnabled != null ?
                propertyResourceEnabled : requestTokenUri != null & accessTokenUri != null;

        if (registerResources) {
            String requestUri = PropertiesHelper.getValue(properties, OAuth1ServerProperties.REQUEST_TOKEN_URI,
                    null, String.class);
            if (requestUri == null) {
                requestUri = requestTokenUri == null ? "requestToken" : requestTokenUri;
            }

            String accessUri = PropertiesHelper.getValue(properties, OAuth1ServerProperties.ACCESS_TOKEN_URI,
                    null, String.class);
            if (accessUri == null) {
                accessUri = accessTokenUri == null ? "accessToken" : accessTokenUri;
            }

            final Resource requestResource = Resource.builder(RequestTokenResource.class).path(requestUri).build();
            final Resource accessResource = Resource.builder(AccessTokenResource.class).path(accessUri).build();

            context.register(new MyModelProcessor(requestResource, accessResource));
        }
        return true;
    }

    @Priority(100)
    private static class MyModelProcessor implements ModelProcessor {
        private final Resource[] resources;

        private MyModelProcessor(Resource... resources) {
            this.resources = resources;
        }

        @Override
        public ResourceModel processResourceModel(ResourceModel resourceModel, Configuration configuration) {
            final ResourceModel.Builder builder = new ResourceModel.Builder(resourceModel, false);
            for (Resource resource : resources) {
                builder.addResource(resource);
            }

            return builder.build();
        }

        @Override
        public ResourceModel processSubResource(ResourceModel subResourceModel, Configuration configuration) {
            return subResourceModel;
        }
    }
	
}

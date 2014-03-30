package edu.jhu.pha.vospace;

import javax.ws.rs.ApplicationPath;

import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.glassfish.jersey.media.sse.SseFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.server.filter.RolesAllowedDynamicFeature;
import org.glassfish.jersey.server.oauth1.OAuth1ServerFeature;

import edu.jhu.pha.vospace.oauth.MyOAuthServerFeature;
import edu.jhu.pha.vospace.rest.OptionsFilter;
import edu.jhu.pha.vospace.rest.SciDriveOAuthProvider;
import edu.jhu.pha.vospace.rest.UpdatesController;

@ApplicationPath("/")
public class VospaceApplication extends ResourceConfig {
	
	final SciDriveOAuthProvider oAuthProvider = new SciDriveOAuthProvider();
    final OAuth1ServerFeature oAuthServerFeature = new OAuth1ServerFeature(oAuthProvider,
            "/request_token", "/access_token");
	
    public VospaceApplication() {
        register(oAuthServerFeature);
        register(OptionsFilter.class);
        register(edu.jhu.pha.vospace.keystone.SciServerAuthFilter.class);
        register(RolesAllowedDynamicFeature.class);
        register(MultiPartFeature.class);
        register(SseFeature.class);
        register(UpdatesController.class);
        packages("edu.jhu.pha.vosync.rest","edu.jhu.pha.vospace.rest");
        
    }
}

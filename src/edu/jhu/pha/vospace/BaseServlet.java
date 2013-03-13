/*******************************************************************************
 * Copyright 2013 Johns Hopkins University
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package edu.jhu.pha.vospace;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import net.oauth.OAuthAccessor;
import net.oauth.OAuthMessage;
import net.oauth.OAuthProblemException;
import net.oauth.server.OAuthServlet;

import org.apache.log4j.Logger;

import edu.jhu.pha.vospace.oauth.MySQLOAuthProvider;

/** A base class for VOSpace servlets that need to do error handling & redirection. */
public abstract class BaseServlet extends HttpServlet {

	private static final long serialVersionUID = -3479515785847664821L;

	/** What page should we send the user to in case of an error?  For many, this will be "/authorize.jsp".
     *  If blank (null or ""), a very simple error page will be created. */
    public abstract String getErrorPage();
    
    private static final Logger logger = Logger.getLogger(BaseServlet.class);

    /** Show an error to the user. Does not log it, though -- assumes it is already logged, if appropriate. */
    public void handleError(HttpServletRequest request, HttpServletResponse response, String error)
            throws IOException, ServletException {
    	logger.debug("Handle Error");

        if (!isBlank(error))
            request.setAttribute("ERROR", error);
        if (!isBlank(getErrorPage()))
            request.getRequestDispatcher(getErrorPage()).forward(request, response);
        // Fall back to displaying the error in its own page -- kind of primitive, with no recourse
        // for the user.  Maybe forward to a full dedicated error page?
        else {
            response.setContentType("text/html");
            response.setCharacterEncoding("UTF-8");
            response.getWriter().println("<html><title>Error - " + error + "</title>");
            response.getWriter().println("<body><h1>Error</h1>");
            response.getWriter().println("<p>" + error + "</p>");
            response.getWriter().println("</body></html>");
        }
    }

    /** Fetch the current OAuth access token from the database. */
    public OAuthAccessor getAccessor(HttpServletRequest request)
            throws IOException, OAuthProblemException
    {
        OAuthMessage requestMessage = OAuthServlet.getMessage(request, null);
        return MySQLOAuthProvider.getAccessor(requestMessage.getToken());
    }

    public static boolean isBlank(String s) { return s == null || s.trim().length() == 0; }
}

/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.router;

import com.facebook.airlift.bootstrap.Bootstrap;
import com.facebook.airlift.event.client.EventModule;
import com.facebook.airlift.http.server.HttpServerModule;
import com.facebook.airlift.jaxrs.JaxrsModule;
import com.facebook.airlift.jmx.JmxHttpModule;
import com.facebook.airlift.jmx.JmxModule;
import com.facebook.airlift.json.JsonModule;
import com.facebook.airlift.log.LogJmxModule;
import com.facebook.airlift.log.Logger;
import com.facebook.airlift.node.NodeModule;
import com.facebook.airlift.tracetoken.TraceTokenModule;
import com.facebook.presto.ClientRequestFilterManager;
import com.facebook.presto.ClientRequestFilterModule;
import com.facebook.presto.router.security.RouterSecurityModule;
import com.facebook.presto.server.security.PasswordAuthenticatorManager;
import com.facebook.presto.server.security.PrestoAuthenticatorManager;
import com.google.common.collect.ImmutableList;
import com.google.inject.Injector;
import com.google.inject.Module;
import org.weakref.jmx.guice.MBeanModule;

public class PrestoRouter
{
    private PrestoRouter()
    {
    }

    public static void start(Module... extraModules)
    {
        Bootstrap app = new Bootstrap(ImmutableList.<Module>builder()
                .add(new NodeModule())
                .add(new HttpServerModule())
                .add(new ClientRequestFilterModule())
                .add(new JsonModule())
                .add(new JaxrsModule(true))
                .add(new MBeanModule())
                .add(new JmxModule())
                .add(new JmxHttpModule())
                .add(new LogJmxModule())
                .add(new TraceTokenModule())
                .add(new EventModule())
                .add(new RouterSecurityModule())
                .add(new RouterModule())
                .add(extraModules)
                .build());

        Logger log = Logger.get(RouterModule.class);
        try {
            Injector injector = app.initialize();
            injector.getInstance(RouterPluginManager.class).loadPlugins();
            injector.getInstance(ClientRequestFilterManager.class).loadClientRequestFilters();
            injector.getInstance(PasswordAuthenticatorManager.class).loadPasswordAuthenticator();
            injector.getInstance(PrestoAuthenticatorManager.class).loadPrestoAuthenticator();
            log.info("======== SERVER STARTED ========");
        }
        catch (Throwable t) {
            log.error(t);
            System.exit(1);
        }
    }

    public static void main(String[] args)
    {
        start();
    }
}

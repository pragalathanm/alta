package org.pb.alta;

import io.quarkus.arc.Unremovable;
import io.quarkus.credentials.CredentialsProvider;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import javax.enterprise.context.ApplicationScoped;

/**
 *
 * @author Pragalathan M <pragalathanm@gmail.com>
 */
@ApplicationScoped
@Unremovable
public class PicocliCredentialsProvider implements CredentialsProvider {

    private char[] password;

    @Override
    public Map<String, String> getCredentials(String credentialsProviderName) {
        Map<String, String> properties = new HashMap<>();
        properties.put(PASSWORD_PROPERTY_NAME, new String(password));
        Arrays.fill(password, ' ');
        password = null;
        return properties;
    }

    public void setPassword(char[] password) {
        this.password = password;
    }
}

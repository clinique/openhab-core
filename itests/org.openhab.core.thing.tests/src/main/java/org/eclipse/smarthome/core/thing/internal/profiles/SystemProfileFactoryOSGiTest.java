/**
 * Copyright (c) 2010-2019 Contributors to the openHAB project
 *
 * See the NOTICE file(s) distributed with this work for additional
 * information.
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License 2.0 which is available at
 * http://www.eclipse.org/legal/epl-2.0
 *
 * SPDX-License-Identifier: EPL-2.0
 */
package org.eclipse.smarthome.core.thing.internal.profiles;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.math.BigDecimal;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.eclipse.smarthome.config.core.Configuration;
import org.eclipse.smarthome.core.library.CoreItemFactory;
import org.eclipse.smarthome.core.thing.Channel;
import org.eclipse.smarthome.core.thing.ChannelUID;
import org.eclipse.smarthome.core.thing.binding.builder.ChannelBuilder;
import org.eclipse.smarthome.core.thing.profiles.ProfileCallback;
import org.eclipse.smarthome.core.thing.profiles.ProfileContext;
import org.eclipse.smarthome.core.thing.profiles.ProfileType;
import org.eclipse.smarthome.core.thing.profiles.ProfileTypeProvider;
import org.eclipse.smarthome.core.thing.profiles.ProfileTypeUID;
import org.eclipse.smarthome.core.thing.profiles.SystemProfiles;
import org.eclipse.smarthome.core.thing.type.ChannelType;
import org.eclipse.smarthome.test.java.JavaOSGiTest;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

/**
 * Test cases for the {@link SystemProfileFactory} class.
 *
 * @author Simon Kaufmann - Initial contribution
 */
public class SystemProfileFactoryOSGiTest extends JavaOSGiTest {

    private final Map<String, Object> properties = new HashMap<String, Object>() {
        private static final long serialVersionUID = 1L;
        {
            put("offset", BigDecimal.ZERO);
        }
    };

    private SystemProfileFactory profileFactory;

    @Mock
    private ProfileCallback mockCallback;

    @Mock
    private ProfileContext mockContext;

    @Before
    public void setUp() {
        profileFactory = getService(ProfileTypeProvider.class, SystemProfileFactory.class);
        assertNotNull(profileFactory);

        mockCallback = mock(ProfileCallback.class);

        mockContext = mock(ProfileContext.class);
        when(mockContext.getConfiguration()).thenReturn(new Configuration(properties));
    }

    @Test
    public void systemProfileTypesAndUidsShouldBeAvailable() {
        Collection<ProfileTypeUID> systemProfileTypeUIDs = profileFactory.getSupportedProfileTypeUIDs();
        assertEquals(15, systemProfileTypeUIDs.size());

        Collection<ProfileType> systemProfileTypes = profileFactory.getProfileTypes(null);
        assertEquals(systemProfileTypeUIDs.size(), systemProfileTypes.size());
    }

    @Test
    public void testFactoryCreatesAvailableProfiles() {
        for (ProfileTypeUID profileTypeUID : profileFactory.getSupportedProfileTypeUIDs()) {
            assertThat(profileFactory.createProfile(profileTypeUID, mockCallback, mockContext), is(notNullValue()));
        }
    }

    @Test
    public void testGetSuggestedProfileTypeUIDNullChannelType1() {
        assertThat(profileFactory.getSuggestedProfileTypeUID((ChannelType) null, CoreItemFactory.SWITCH),
                is(nullValue()));
    }

    @Test
    public void testGetSuggestedProfileTypeUIDNullChannelType2() {
        Channel channel = ChannelBuilder.create(new ChannelUID("test:test:test:test"), CoreItemFactory.SWITCH).build();
        assertThat(profileFactory.getSuggestedProfileTypeUID(channel, CoreItemFactory.SWITCH),
                is(SystemProfiles.DEFAULT));
    }
}

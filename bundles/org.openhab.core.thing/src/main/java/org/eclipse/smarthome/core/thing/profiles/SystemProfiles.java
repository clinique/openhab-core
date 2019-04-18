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
package org.eclipse.smarthome.core.thing.profiles;

import org.eclipse.jdt.annotation.NonNullByDefault;
import org.eclipse.smarthome.core.library.CoreItemFactory;
import org.eclipse.smarthome.core.thing.DefaultSystemChannelTypeProvider;

/**
 * System profile constants.
 *
 * @author Simon Kaufmann - Initial contribution
 */
@NonNullByDefault
public interface SystemProfiles {

    ProfileTypeUID DEFAULT = new ProfileTypeUID(ProfileTypeUID.SYSTEM_SCOPE, "default");
    ProfileTypeUID FOLLOW = new ProfileTypeUID(ProfileTypeUID.SYSTEM_SCOPE, "follow");
    ProfileTypeUID OFFSET = new ProfileTypeUID(ProfileTypeUID.SYSTEM_SCOPE, "offset");
    ProfileTypeUID RAWBUTTON_TOGGLE_PLAYER = new ProfileTypeUID(ProfileTypeUID.SYSTEM_SCOPE, "rawbutton-toggle-player");
    ProfileTypeUID RAWBUTTON_TOGGLE_SWITCH = new ProfileTypeUID(ProfileTypeUID.SYSTEM_SCOPE, "rawbutton-toggle-switch");
    ProfileTypeUID RAWROCKER_DIMMER = new ProfileTypeUID(ProfileTypeUID.SYSTEM_SCOPE, "rawrocker-to-dimmer");
    ProfileTypeUID RAWROCKER_NEXT_PREVIOUS = new ProfileTypeUID(ProfileTypeUID.SYSTEM_SCOPE,
            "rawrocker-to-next-previous");
    ProfileTypeUID RAWROCKER_ON_OFF = new ProfileTypeUID(ProfileTypeUID.SYSTEM_SCOPE, "rawrocker-to-on-off");
    ProfileTypeUID RAWROCKER_PLAY_PAUSE = new ProfileTypeUID(ProfileTypeUID.SYSTEM_SCOPE, "rawrocker-to-play-pause");
    ProfileTypeUID RAWROCKER_REWIND_FASTFORWARD = new ProfileTypeUID(ProfileTypeUID.SYSTEM_SCOPE,
            "rawrocker-to-rewind-fastforward");
    ProfileTypeUID TIMESTAMP_CHANGE = new ProfileTypeUID(ProfileTypeUID.SYSTEM_SCOPE, "timestamp-change");
    ProfileTypeUID TIMESTAMP_UPDATE = new ProfileTypeUID(ProfileTypeUID.SYSTEM_SCOPE, "timestamp-update");
    ProfileTypeUID EXPIRE = new ProfileTypeUID(ProfileTypeUID.SYSTEM_SCOPE, "expire");

    StateProfileType DEFAULT_TYPE = ProfileTypeBuilder.newState(DEFAULT, "Default").build();

    StateProfileType FOLLOW_TYPE = ProfileTypeBuilder.newState(FOLLOW, "Follow").build();

    StateProfileType OFFSET_TYPE = ProfileTypeBuilder.newState(OFFSET, "Offset")
            .withSupportedItemTypes(CoreItemFactory.NUMBER).withSupportedItemTypesOfChannel(CoreItemFactory.NUMBER)
            .build();

    TriggerProfileType RAWBUTTON_TOGGLE_PLAYER_TYPE = ProfileTypeBuilder
            .newTrigger(RAWBUTTON_TOGGLE_PLAYER, "Raw Button Toggle Player")
            .withSupportedItemTypes(CoreItemFactory.PLAYER)
            .withSupportedChannelTypeUIDs(DefaultSystemChannelTypeProvider.SYSTEM_RAWBUTTON.getUID()).build();

    TriggerProfileType RAWBUTTON_TOGGLE_SWITCH_TYPE = ProfileTypeBuilder
            .newTrigger(RAWBUTTON_TOGGLE_SWITCH, "Raw Button Toggle Switch")
            .withSupportedItemTypes(CoreItemFactory.SWITCH, CoreItemFactory.DIMMER, CoreItemFactory.COLOR)
            .withSupportedChannelTypeUIDs(DefaultSystemChannelTypeProvider.SYSTEM_RAWBUTTON.getUID()).build();

    TriggerProfileType RAWROCKER_ON_OFF_TYPE = ProfileTypeBuilder.newTrigger(RAWROCKER_ON_OFF, "Raw Rocker To On Off")
            .withSupportedItemTypes(CoreItemFactory.SWITCH, CoreItemFactory.DIMMER)
            .withSupportedChannelTypeUIDs(DefaultSystemChannelTypeProvider.SYSTEM_RAWROCKER.getUID()).build();

    TriggerProfileType RAWROCKER_DIMMER_TYPE = ProfileTypeBuilder.newTrigger(RAWROCKER_DIMMER, "Raw Rocker To Dimmer")
            .withSupportedItemTypes(CoreItemFactory.DIMMER)
            .withSupportedChannelTypeUIDs(DefaultSystemChannelTypeProvider.SYSTEM_RAWROCKER.getUID()).build();

    TriggerProfileType RAWROCKER_NEXT_PREVIOUS_TYPE = ProfileTypeBuilder
            .newTrigger(RAWROCKER_NEXT_PREVIOUS, "Raw Rocker To Next/Previous")
            .withSupportedItemTypes(CoreItemFactory.PLAYER)
            .withSupportedChannelTypeUIDs(DefaultSystemChannelTypeProvider.SYSTEM_RAWROCKER.getUID()).build();

    TriggerProfileType RAWROCKER_PLAY_PAUSE_TYPE = ProfileTypeBuilder
            .newTrigger(RAWROCKER_PLAY_PAUSE, "Raw Rocker To Play/Pause").withSupportedItemTypes(CoreItemFactory.PLAYER)
            .withSupportedChannelTypeUIDs(DefaultSystemChannelTypeProvider.SYSTEM_RAWROCKER.getUID()).build();

    TriggerProfileType RAWROCKER_REWIND_FASTFORWARD_TYPE = ProfileTypeBuilder
            .newTrigger(RAWROCKER_REWIND_FASTFORWARD, "Raw Rocker To Rewind/Fastforward")
            .withSupportedItemTypes(CoreItemFactory.PLAYER)
            .withSupportedChannelTypeUIDs(DefaultSystemChannelTypeProvider.SYSTEM_RAWROCKER.getUID()).build();

    StateProfileType TIMESTAMP_CHANGE_TYPE = ProfileTypeBuilder.newState(TIMESTAMP_CHANGE, "Timestamp on change")
            .withSupportedItemTypes(CoreItemFactory.DATETIME).build();

    StateProfileType TIMESTAMP_UPDATE_TYPE = ProfileTypeBuilder.newState(TIMESTAMP_UPDATE, "Timestamp on update")
            .withSupportedItemTypes(CoreItemFactory.DATETIME).build();

    StateProfileType EXPIRE_TYPE = ProfileTypeBuilder.newState(EXPIRE, "Expire").build();

}

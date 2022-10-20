package org.tron.core.vm.utils;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.tron.core.actuator.UnfreezeBalanceV2Actuator;
import org.tron.core.capsule.AccountCapsule;
import org.tron.core.capsule.DelegatedResourceCapsule;
import org.tron.core.vm.config.VMConfig;
import org.tron.core.vm.repository.Repository;
import org.tron.protos.Protocol;

public class FreezeV2Util {

  private FreezeV2Util() {
  }

  public static long queryExpireUnfreezeBalanceV2(byte[] address, long time, Repository repository) {
    if (!VMConfig.allowTvmFreezeV2()) {
      return 0;
    }

    // time should be transferred to slot.
    long slot = repository.getSlotByTimestampMs(time);
    AccountCapsule accountCapsule = repository.getAccount(address);
    if (accountCapsule == null) {
      return 0;
    }

    List<Protocol.Account.UnFreezeV2> unfrozenV2List =
        accountCapsule.getInstance().getUnfrozenV2List();

    return getTotalWithdrawUnfreeze(unfrozenV2List, slot);
  }

  public static long queryUnfreezableBalanceV2(byte[] address, long type, Repository repository) {
    if (!VMConfig.allowTvmFreezeV2()) {
      return 0;
    }

    AccountCapsule accountCapsule = repository.getAccount(address);
    if (accountCapsule == null) {
      return 0;
    }

    // BANDWIDTH
    if (type == 0) {
      return accountCapsule.getFrozenV2BalanceForBandwidth();
    }

    // ENERGY
    if (type == 1) {
      return accountCapsule.getFrozenV2BalanceForEnergy();
    }

    // POWER
    if (type == 2) {
      return accountCapsule.getTronPowerFrozenV2Balance();
    }

    return 0;
  }

  // only freezeV2.
  public static long queryResourceV2(byte[] from, byte[] to, long type, Repository repository) {
    if (!VMConfig.allowTvmFreezeV2()) {
      return 0;
    }

    byte[] key = DelegatedResourceCapsule.createDbKeyV2(from, to);
    DelegatedResourceCapsule delegatedResource = repository.getDelegatedResource(key);
    if (delegatedResource == null) {
      return 0;
    }

    // BANDWIDTH
    if (type == 0) {
      return delegatedResource.getFrozenBalanceForBandwidth();
    }

    // ENERGY
    if (type == 1) {
      return delegatedResource.getFrozenBalanceForEnergy();
    }

    return 0;
  }

  public static Pair<Long, Long> queryFrozenBalanceUsage(byte[] address, long type, Repository repository) {
    if (!VMConfig.allowTvmFreezeV2()) {
      return Pair.of(0L, 0L);
    }

    AccountCapsule accountCapsule = repository.getAccount(address);
    if (accountCapsule == null) {
      return Pair.of(0L, 0L);
    }

    if (type == 0) {
      return repository.getAccountNetUsageBalanceAndRestoreSeconds(accountCapsule);
    } else if (type == 1) {
      return repository.getAccountEnergyUsageBalanceAndRestoreSeconds(accountCapsule);
    }

    return Pair.of(0L, 0L);
  }

  public static long queryAvailableUnfreezeV2Size(byte[] address, Repository repository) {
    if (!VMConfig.allowTvmFreezeV2()) {
      return 0L;
    }

    AccountCapsule accountCapsule = repository.getAccount(address);
    if (accountCapsule == null) {
      return 0L;
    }

    long now = repository.getHeadSlot();
    int unfreezingV2Count = accountCapsule.getUnfreezingV2Count(now);
    return Long.max(UnfreezeBalanceV2Actuator.getUNFREEZE_MAX_TIMES() - unfreezingV2Count, 0L);
  }

  public static long queryDelegatableResource(byte[] address, long type, Repository repository) {
    if (!VMConfig.allowTvmFreezeV2()) {
      return 0L;
    }

    AccountCapsule accountCapsule = repository.getAccount(address);
    if (accountCapsule == null) {
      return 0L;
    }

    if (type == 0) {
      // self frozenV2 resource
      long frozenV2Resource = accountCapsule.getFrozenV2BalanceForBandwidth();

      // total Usage.
      Pair<Long, Long> usagePair =
          repository.getAccountNetUsageBalanceAndRestoreSeconds(accountCapsule);
      if (usagePair == null || usagePair.getLeft() == null) {
        return frozenV2Resource;
      }

      long usage = usagePair.getLeft();
      if (usage <= 0) {
        return frozenV2Resource;
      }

      // total resource
      long totalResource = accountCapsule.getAllFrozenBalanceForBandwidth();
      if (totalResource <= usage) {
        return 0L;
      }

      return frozenV2Resource * (totalResource - usage) / totalResource;
    }

    if (type == 1) {
      // self frozenV2 resource
      long frozenV2Resource = accountCapsule.getFrozenV2BalanceForEnergy();

      // total Usage.
      Pair<Long, Long> usagePair =
          repository.getAccountEnergyUsageBalanceAndRestoreSeconds(accountCapsule);
      if (usagePair == null || usagePair.getLeft() == null) {
        return frozenV2Resource;
      }

      long usage = usagePair.getLeft();
      if (usage <= 0) {
        return frozenV2Resource;
      }

      // total resource
      long totalResource = accountCapsule.getAllFrozenBalanceForEnergy();
      if (totalResource <= usage) {
        return 0L;
      }

      return frozenV2Resource * (totalResource - usage) / totalResource;
    }

    return 0L;
  }

  public static Triple<Long, Long, Long> checkUndelegateResource(byte[] address, long amount, long type, Repository repository) {
    if (!VMConfig.allowTvmFreezeV2()) {
      return Triple.of(0L, 0L, 0L);
    }

    if (amount <= 0) {
      return Triple.of(0L, 0L, 0L);
    }

    AccountCapsule accountCapsule = repository.getAccount(address);
    if (accountCapsule == null) {
      return Triple.of(0L, 0L, 0L);
    }

    Pair<Long, Long> usagePair;
    long resourceLimit;
    if (type == 0) {
      usagePair = repository.getAccountNetUsageBalanceAndRestoreSeconds(accountCapsule);
      resourceLimit = accountCapsule.getAllFrozenBalanceForBandwidth();
    } else if (type == 1) {
      usagePair = repository.getAccountEnergyUsageBalanceAndRestoreSeconds(accountCapsule);
      resourceLimit = accountCapsule.getAllFrozenBalanceForEnergy();
    } else {
      return Triple.of(0L, 0L, 0L);
    }

    if (usagePair == null || usagePair.getLeft() == null || usagePair.getRight() == null) {
      return Triple.of(0L, 0L, 0L);
    }

    amount = Math.min(amount, resourceLimit);
    if (resourceLimit <= usagePair.getLeft()) {
      return Triple.of(0L, amount, usagePair.getRight());
    }

    long clean = (long) (amount * ((double) (resourceLimit - usagePair.getLeft()) / resourceLimit));

    return Triple.of(clean, amount - clean, usagePair.getRight());
  }

  private static long getTotalWithdrawUnfreeze(List<Protocol.Account.UnFreezeV2> unfrozenV2List, long time) {
    return getTotalWithdrawList(unfrozenV2List, time).stream()
        .mapToLong(Protocol.Account.UnFreezeV2::getUnfreezeAmount).sum();
  }

  private static List<Protocol.Account.UnFreezeV2> getTotalWithdrawList(List<Protocol.Account.UnFreezeV2> unfrozenV2List, long now) {
    return unfrozenV2List.stream().filter(unfrozenV2 -> (unfrozenV2.getUnfreezeAmount() > 0
        && unfrozenV2.getUnfreezeExpireTime() <= now)).collect(Collectors.toList());
  }

}
